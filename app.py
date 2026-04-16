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
    classify_and_tag,
    sentiment_batch_ai,
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
        cfg["reddit"] = {**cfg["reddit"], **body["reddit"]}
    if "dc" in body:
        cfg["dc"] = {**cfg["dc"], **body["dc"]}
    if "primary_keywords" in body:
        cfg["primary_keywords"] = body["primary_keywords"]
    if "secondary_keywords" in body:
        cfg["secondary_keywords"] = body["secondary_keywords"]
    if "primary_aliases" in body:
        cfg["primary_aliases"] = {**cfg.get("primary_aliases", {}), **body["primary_aliases"]}
    if "secondary_aliases" in body:
        cfg["secondary_aliases"] = {**cfg.get("secondary_aliases", {}), **body["secondary_aliases"]}
    cfg["use_ai_sentiment"] = body.get("use_ai_sentiment", True)

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
        classified = classify_and_tag(all_posts, cfg)

        if not classified:
            update_state(
                status="error",
                message="키워드 매칭된 게시글이 없습니다",
                progress=100,
                last_run=datetime.utcnow().isoformat(),
                error="matched_zero",
            )
            return jsonify({"error": "키워드 매칭된 게시글이 없습니다", "collected": len(all_posts), "items": []}), 200

        update_state(status="analyzing", message=f"AI 감성분석 중... ({len(classified)}건)", progress=70)
        sentiments = sentiment_batch_ai(classified, cfg)
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
