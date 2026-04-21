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
    fetch_dc_details,
    filter_by_keywords,
    sentiment_batch,
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


@app.route("/dc-detail", methods=["POST", "OPTIONS"])
def dc_detail():
    """DC갤러리 상세 페이지 본문+댓글 수집. URL 배열을 받아서 처리."""
    if request.method == "OPTIONS":
        return "", 204
    try:
        body = request.get_json() or {}
    except Exception:
        body = {}

    urls = body.get("urls", [])
    if not urls:
        return jsonify({"error": "urls가 필요합니다"}), 400

    print(f"[dc-detail] {len(urls)}건 상세 수집 요청")
    details = fetch_dc_details(urls)

    return jsonify({"ok": True, "details": details, "count": len(details)})


@app.route("/report", methods=["POST", "OPTIONS"])
def report():
    """분석 보고서 API: 크롤링된 전체 게시글을 Gemini에게 보내서 종합 보고서 생성."""
    if request.method == "OPTIONS":
        return "", 204
    try:
        body = request.get_json() or {}
    except Exception:
        body = {}

    items = body.get("items", [])
    if not items:
        return jsonify({"error": "items가 필요합니다"}), 400

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return jsonify({"error": "GEMINI_API_KEY가 설정되지 않았습니다"}), 400

    # 게시글을 텍스트로 정리 (최대 4000자) — URL 포함
    posts_text = ""
    for i, p in enumerate(items[:50]):
        source = p.get("source", "")
        ptype = "본문" if p.get("type") == "post" else "댓글"
        sentiment = p.get("sentiment", "")
        tags = ", ".join(p.get("tags", []))
        text = (p.get("text_ko") or p.get("text", ""))[:150]
        url = p.get("url", "")
        posts_text += f"[#{i+1}] [{source}] [{ptype}] [{sentiment}] [{tags}] {text} (URL: {url})\n"
        if len(posts_text) > 4000:
            break

    prompt = f"""아래는 게임 '퍼스트 디센던트(The First Descendant)' 커뮤니티(Reddit, DC갤러리)에서 수집한 게시글 {len(items)}건입니다.

이 데이터를 분석하여 한국어로 보고서를 작성해주세요:

1. **전체 요약** (3~5문장으로 현재 커뮤니티 분위기 정리)
2. **키워드별 분석** (자주 등장하는 키워드별로 유저 피드백 정리)
3. **긍정 의견** (유저들이 좋아하는 점 3~5개)
4. **부정 의견** (유저들이 불만인 점 3~5개)
5. **개선 요청** (유저들이 원하는 변화 3~5개)
6. **주목할 트렌드** (특이 사항이나 급부상 이슈)

중요한 규칙:
- 마크다운 형식으로 작성
- 각 분석 항목에서 유저 의견을 인용할 때, 반드시 해당 게시글 번호와 URL을 함께 표기
- 인용 형식: "유저 의견 내용" ([#N](URL))  (N은 게시글 번호)
- 예시: "포탑 시스템이 너무 약하다" ([#3](https://www.reddit.com/r/...))

게시글 데이터:
{posts_text}"""

    try:
        import requests as req
        r = req.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key={api_key}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 2000, "temperature": 0.3},
            },
            timeout=60,
        )
        if r.status_code != 200:
            return jsonify({"error": f"Gemini API 에러 {r.status_code}"}), 500

        data = r.json()
        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            if parts:
                report_text = parts[0].get("text", "").strip()
                return jsonify({"ok": True, "report": report_text})

        return jsonify({"error": "보고서 생성 실패"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
