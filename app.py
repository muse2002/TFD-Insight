"""
TFD Insight Crawler API (Render 배포용)
=========================================
Flask 서버 — 웹 버튼 클릭으로 크롤링 → JSON 반환

엔드포인트:
    GET  /              — 헬스체크 (JSON 상태)
    POST /crawl         — 크롤링 실행
    GET  /crawl/status  — 최근 크롤링 상태 조회

배포 가이드는 README.md 참고.
"""

import os
import json
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

from crawler import (
    crawl_reddit,
    crawl_dc,
    filter_by_keywords,
    sentiment_batch,
    translate_posts,
    summarize_posts,
    extract_keywords_from_posts,
    DEFAULT_CONFIG,
)

app = Flask(__name__)
CORS(app)   # 모든 origin 허용 (GitHub Pages 등 어디서든 호출 가능)

# 서버 상태 (메모리 저장, 재시작 시 초기화)
STATE = {
    "status": "idle",       # idle | crawling | analyzing | done | error
    "message": "",
    "progress": 0,          # 0 ~ 100
    "last_run": None,
    "last_result": None,    # 마지막 크롤링 결과 (JSON)
    "error": None,
}
STATE_LOCK = threading.Lock()


def update_state(**kwargs):
    with STATE_LOCK:
        STATE.update(kwargs)


@app.route("/")
def health():
    return jsonify({
        "service": "TFD Insight Crawler",
        "version": "1.0",
        "status": "ok",
        "endpoints": {
            "crawl": "POST /crawl",
            "status": "GET /crawl/status",
        },
    })


@app.route("/crawl/status")
def crawl_status():
    with STATE_LOCK:
        return jsonify({
            "status": STATE["status"],
            "message": STATE["message"],
            "progress": STATE["progress"],
            "last_run": STATE["last_run"],
            "count": len(STATE["last_result"]) if STATE["last_result"] else 0,
            "error": STATE["error"],
        })


@app.route("/crawl", methods=["POST", "OPTIONS"])
def crawl():
    if request.method == "OPTIONS":
        return "", 204

    # 이미 크롤링 중이면 거부
    with STATE_LOCK:
        if STATE["status"] in ("crawling", "analyzing"):
            return jsonify({"error": "이미 크롤링이 진행 중입니다", "status": STATE["status"]}), 409

    # 요청 body에서 설정 받기 (기본값: DEFAULT_CONFIG)
    try:
        body = request.get_json() or {}
    except Exception:
        body = {}

    # 설정 병합
    cfg = {**DEFAULT_CONFIG}
    if "reddit" in body:
        reddit_cfg = {**cfg["reddit"], **body["reddit"]}
        if "start" in reddit_cfg and "date_start" not in reddit_cfg:
            reddit_cfg["date_start"] = reddit_cfg.pop("start")
        if "end" in reddit_cfg and "date_end" not in reddit_cfg:
            reddit_cfg["date_end"] = reddit_cfg.pop("end")
        cfg["reddit"] = reddit_cfg
    if "dc" in body:
        dc_cfg = {**cfg["dc"], **body["dc"]}
        if "start" in dc_cfg and "date_start" not in dc_cfg:
            dc_cfg["date_start"] = dc_cfg.pop("start")
        if "end" in dc_cfg and "date_end" not in dc_cfg:
            dc_cfg["date_end"] = dc_cfg.pop("end")
        cfg["dc"] = dc_cfg

    # 키워드 (평면 리스트, OR 매칭)
    if "keywords" in body and isinstance(body["keywords"], list):
        cfg["keywords"] = body["keywords"]

    # 번역 설정
    if "translate_english" in body:
        cfg["translate_english"] = bool(body["translate_english"])

    # 동기 실행 (Render Free는 타임아웃이 길지 않으므로 빠르게 처리)
    try:
        update_state(status="crawling", message="Reddit 수집 중...", progress=10, error=None)

        all_posts = []
        if cfg["reddit"].get("enabled", True):
            all_posts.extend(crawl_reddit(cfg["reddit"]))

        update_state(message="DC갤러리 수집 중...", progress=35)
        if cfg["dc"].get("enabled", True):
            all_posts.extend(crawl_dc(cfg["dc"]))

        update_state(message=f"키워드 매칭 중... ({len(all_posts)}건)", progress=55)
        keywords = cfg.get("keywords", [])
        classified = filter_by_keywords(all_posts, keywords)

        if not classified:
            update_state(
                status="error",
                message="키워드 매칭된 게시글이 없습니다",
                progress=100,
                last_run=datetime.utcnow().isoformat(),
                error="matched_zero",
            )
            return jsonify({"error": "키워드 매칭된 게시글이 없습니다", "collected": len(all_posts), "items": []}), 200

        update_state(status="analyzing", message=f"감성분석 중... ({len(classified)}건)", progress=70)
        sentiments = sentiment_batch(classified, cfg)
        for p, s in zip(classified, sentiments):
            p["sentiment"] = s if s in ("긍정", "부정", "개선") else "개선"

        for i, p in enumerate(classified):
            p["id"] = i + 1

        update_state(
            status="done",
            message=f"완료: {len(classified)}건",
            progress=100,
            last_run=datetime.utcnow().isoformat(),
            last_result=classified,
            error=None,
        )

        return jsonify({
            "ok": True,
            "count": len(classified),
            "items": classified,
            "stats": {
                "collected_total": len(all_posts),
                "after_keyword_match": len(classified),
                "reddit": sum(1 for p in classified if p["source"] == "Reddit"),
                "dc": sum(1 for p in classified if p["source"] == "DC갤러리"),
                "positive": sum(1 for p in classified if p["sentiment"] == "긍정"),
                "negative": sum(1 for p in classified if p["sentiment"] == "부정"),
                "improvement": sum(1 for p in classified if p["sentiment"] == "개선"),
            },
            "run_at": datetime.utcnow().isoformat(),
        })

    except Exception as e:
        err_msg = str(e)
        update_state(
            status="error",
            message=f"에러: {err_msg}",
            progress=0,
            last_run=datetime.utcnow().isoformat(),
            error=err_msg,
        )
        return jsonify({"error": err_msg}), 500


@app.route("/discover", methods=["POST", "OPTIONS"])
def discover():
    """키워드 발견 API:
    Reddit/DC갤러리에서 게시글을 수집한 뒤, 자주 등장하는 단어를 빈도순으로 반환.
    시드 키워드 없이 전체 게시글에서 추출.
    """
    if request.method == "OPTIONS":
        return "", 204

    try:
        body = request.get_json() or {}
    except Exception:
        body = {}

    cfg = {**DEFAULT_CONFIG}
    if "reddit" in body:
        reddit_cfg = {**cfg["reddit"], **body["reddit"]}
        if "start" in reddit_cfg and "date_start" not in reddit_cfg:
            reddit_cfg["date_start"] = reddit_cfg.pop("start")
        if "end" in reddit_cfg and "date_end" not in reddit_cfg:
            reddit_cfg["date_end"] = reddit_cfg.pop("end")
        cfg["reddit"] = reddit_cfg
    if "dc" in body:
        dc_cfg = {**cfg["dc"], **body["dc"]}
        if "start" in dc_cfg and "date_start" not in dc_cfg:
            dc_cfg["date_start"] = dc_cfg.pop("start")
        if "end" in dc_cfg and "date_end" not in dc_cfg:
            dc_cfg["date_end"] = dc_cfg.pop("end")
        cfg["dc"] = dc_cfg

    top_n = body.get("top_n", 40)
    # 결과에서 제외할 단어 (이미 알고 있는 키워드)
    exclude_words = body.get("exclude_words", [])

    try:
        update_state(status="crawling", message="키워드 발견: 게시글 수집 중...", progress=20, error=None)

        all_posts = []
        if cfg["reddit"].get("enabled", True):
            all_posts.extend(crawl_reddit(cfg["reddit"]))
        if cfg["dc"].get("enabled", True):
            all_posts.extend(crawl_dc(cfg["dc"]))

        print(f"[discover] 총 수집: {len(all_posts)}건")

        if not all_posts:
            update_state(status="done", message="수집된 게시글 없음", progress=100)
            return jsonify({"error": "수집된 게시글이 없습니다", "keywords": [], "post_count": 0}), 200

        update_state(message=f"키워드 추출 중... ({len(all_posts)}건)", progress=70)
        discovered = extract_keywords_from_posts(all_posts, seed_keywords=exclude_words, top_n=top_n)

        update_state(status="done", message=f"키워드 {len(discovered)}개 발견", progress=100)

        return jsonify({
            "ok": True,
            "keywords": discovered,
            "post_count": len(all_posts),
        })

    except Exception as e:
        err_msg = str(e)
        update_state(status="error", message=f"에러: {err_msg}", progress=0, error=err_msg)
        return jsonify({"error": err_msg}), 500


@app.route("/translate", methods=["POST", "OPTIONS"])
def translate():
    """번역 API: 게시글 배열을 받아서 영어 본문에 한국어 번역 추가 후 반환."""
    if request.method == "OPTIONS":
        return "", 204
    try:
        body = request.get_json() or {}
    except Exception:
        body = {}

    items = body.get("items", [])
    if not items:
        return jsonify({"error": "items가 필요합니다"}), 400

    print(f"[translate] {len(items)}건 번역 요청")
    translate_posts(items)

    return jsonify({"ok": True, "items": items, "count": len(items)})


@app.route("/summarize", methods=["POST", "OPTIONS"])
def summarize():
    """요약 API: 게시글 배열을 받아서 AI 요약 추가 후 반환."""
    if request.method == "OPTIONS":
        return "", 204
    try:
        body = request.get_json() or {}
    except Exception:
        body = {}

    items = body.get("items", [])
    if not items:
        return jsonify({"error": "items가 필요합니다"}), 400

    print(f"[summarize] {len(items)}건 요약 요청")
    summarize_posts(items)

    return jsonify({"ok": True, "items": items, "count": len(items)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
