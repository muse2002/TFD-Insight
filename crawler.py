"""
TFD Insight Crawler
====================
Reddit + DC갤러리 게시글을 크롤링해서 Claude API로 감성 분석 후 JSON 출력.

사용법:
    1) 의존성 설치
       pip install requests beautifulsoup4 anthropic

    2) 환경변수 설정 (선택, AI 감성분석 사용 시)
       Windows: set ANTHROPIC_API_KEY=sk-ant-...
       Mac/Linux: export ANTHROPIC_API_KEY=sk-ant-...

    3) config.json 편집 후 실행
       python crawler.py

    4) 생성된 data.json을 TFD Insight HTML의 Upload JSON 버튼에 올리기
"""

import json
import time
import re
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("필수 패키지를 설치해주세요: pip install requests beautifulsoup4")
    sys.exit(1)

# Anthropic SDK는 옵션 (없으면 키워드 룰 분석으로 폴백)
try:
    from anthropic import Anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


# =====================================================
# 설정 파일 관리
# =====================================================
DEFAULT_CONFIG = {
    "reddit": {
        "enabled": True,
        "subreddit": "TheFirstDescendant",
        "sort": "new",           # new | hot | top
        "time_filter": "week",   # hour | day | week | month | year | all (sort=top일 때만)
        "limit": 50,
        "date_start": "2026-04-09",
        "date_end": "2026-04-16",
    },
    "dc": {
        "enabled": True,
        "gallery_id": "first_descendant",   # DC 갤러리 id (실제 값으로 교체 필요)
        "pages": 3,
        "date_start": "2026-04-09",
        "date_end": "2026-04-16",
    },
    "primary_keywords": ["타워디펜스", "밸런스", "신규컨텐츠"],
    "secondary_keywords": ["포탑", "바리케이트", "웨이브", "스킬", "캐릭터", "던전", "보상"],
    "primary_aliases": {
        "타워디펜스": ["tower defense", "td", "방어전", "수성"],
        "밸런스": ["balance", "balancing", "nerf", "buff", "너프", "버프"],
        "신규컨텐츠": ["new content", "update", "patch", "업데이트", "패치"],
    },
    "secondary_aliases": {
        "포탑": ["turret", "터렛"],
        "바리케이트": ["barricade", "바리케이드", "벽"],
        "웨이브": ["wave"],
        "스킬": ["skill", "ability"],
        "캐릭터": ["character", "descendant", "계승자"],
        "던전": ["dungeon", "레이드"],
        "보상": ["reward", "drop", "loot", "드랍", "드롭"],
    },
    "use_ai_sentiment": True,      # Claude API 사용 여부
    "claude_model": "claude-opus-4-7",
    "output_path": "data.json",
}


def load_or_create_config(path="config.json"):
    p = Path(path)
    if not p.exists():
        p.write_text(json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[config] {path} 파일을 생성했습니다. 편집 후 다시 실행하세요.")
        sys.exit(0)
    return json.loads(p.read_text(encoding="utf-8"))


# =====================================================
# Reddit 크롤러 (공식 JSON API, 인증 불필요)
# =====================================================
def crawl_reddit(cfg):
    if not cfg.get("enabled"):
        return []
    sub = cfg["subreddit"]
    sort = cfg.get("sort", "new")
    limit = cfg.get("limit", 50)
    print(f"[reddit] r/{sub} {sort} {limit}건 수집 중...")

    params = {"limit": limit, "raw_json": 1}
    if sort == "top":
        params["t"] = cfg.get("time_filter", "week")

    url = f"https://www.reddit.com/r/{sub}/{sort}.json?{urlencode(params)}"
    headers = {"User-Agent": "tfd-insight-crawler/1.0"}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[reddit] 요청 실패: {e}")
        return []

    posts = []
    date_start = datetime.fromisoformat(cfg["date_start"])
    date_end = datetime.fromisoformat(cfg["date_end"]) + timedelta(days=1)

    for item in r.json().get("data", {}).get("children", []):
        d = item.get("data", {})
        created = datetime.fromtimestamp(d.get("created_utc", 0))
        if not (date_start <= created < date_end):
            continue
        title = d.get("title", "")
        body = d.get("selftext", "")
        text = f"{title}\n\n{body}".strip() if body else title
        posts.append({
            "source": "Reddit",
            "text": text,
            "date": created.strftime("%Y-%m-%d"),
            "upvotes": d.get("score", 0),
            "url": "https://www.reddit.com" + d.get("permalink", ""),
        })
    print(f"[reddit] {len(posts)}건 수집 완료")
    return posts


# =====================================================
# DC갤러리 크롤러 (모바일 페이지 HTML 파싱)
# =====================================================
def crawl_dc(cfg):
    if not cfg.get("enabled"):
        return []
    gall_id = cfg["gallery_id"]
    pages = cfg.get("pages", 3)
    print(f"[dc] {gall_id} {pages}페이지 수집 중...")

    posts = []
    date_start = datetime.fromisoformat(cfg["date_start"]).date()
    date_end = datetime.fromisoformat(cfg["date_end"]).date()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Referer": "https://m.dcinside.com/",
    })

    for page in range(1, pages + 1):
        list_url = f"https://m.dcinside.com/board/{gall_id}?page={page}"
        try:
            r = session.get(list_url, timeout=15)
            if r.status_code != 200:
                print(f"[dc] page {page} 응답 {r.status_code}, 중단")
                break
        except Exception as e:
            print(f"[dc] page {page} 요청 실패: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        # 게시글 링크 찾기 (모바일 레이아웃 기준, 실제 셀렉터는 시점에 따라 조정 필요)
        links = soup.select("a.gall-detail-lnktb, a[href*='/board/'][href*='/view/']")
        found_in_page = 0

        for a in links:
            href = a.get("href", "")
            if "/view/" not in href and "no=" not in href:
                continue
            if not href.startswith("http"):
                href = "https://m.dcinside.com" + href

            try:
                detail = session.get(href, timeout=15)
                if detail.status_code != 200:
                    continue
            except Exception:
                continue

            dsoup = BeautifulSoup(detail.text, "html.parser")

            title_el = dsoup.select_one("span.tit, h3, .gallview-tit-box .tit")
            body_el = dsoup.select_one(".thum-txtin, .gallview-content, .write_div")
            date_el = dsoup.select_one(".gall-date, .date")
            rec_el = dsoup.select_one(".btn_recommend .num, .rec-num")

            title = title_el.get_text(strip=True) if title_el else ""
            body = body_el.get_text("\n", strip=True) if body_el else ""
            text = f"{title}\n\n{body}".strip() if body else title
            if not text:
                continue

            # 날짜 파싱 (형식이 다양함: 2026.04.15 / 2026-04-15 / 04.15 등)
            date_str = date_el.get_text(strip=True) if date_el else ""
            post_date = parse_dc_date(date_str)
            if post_date is None:
                continue
            if not (date_start <= post_date <= date_end):
                continue

            upvotes_raw = rec_el.get_text(strip=True) if rec_el else "0"
            try:
                upvotes = int(re.sub(r"[^\d]", "", upvotes_raw) or 0)
            except ValueError:
                upvotes = 0

            posts.append({
                "source": "DC갤러리",
                "text": text,
                "date": post_date.strftime("%Y-%m-%d"),
                "upvotes": upvotes,
                "url": href,
            })
            found_in_page += 1
            time.sleep(0.4)   # 과도한 요청 방지

        print(f"[dc] page {page}: {found_in_page}건")
        if found_in_page == 0 and page > 1:
            break
        time.sleep(0.8)

    print(f"[dc] 총 {len(posts)}건 수집 완료")
    return posts


def parse_dc_date(date_str):
    """DC갤러리 날짜 포맷 다수 처리"""
    if not date_str:
        return None
    s = date_str.strip()
    now = datetime.now().date()

    # "HH:MM" → 오늘
    if re.match(r"^\d{2}:\d{2}$", s):
        return now

    # "MM.DD" → 올해
    m = re.match(r"^(\d{1,2})\.(\d{1,2})$", s)
    if m:
        return datetime(now.year, int(m.group(1)), int(m.group(2))).date()

    # "YYYY.MM.DD" 또는 "YYYY-MM-DD"
    m = re.match(r"^(\d{4})[.\-](\d{1,2})[.\-](\d{1,2})", s)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()

    return None


# =====================================================
# 키워드 매칭 (1차 필터 + 2차 태깅, OR 매칭)
# =====================================================
def match_keywords(text, keywords, aliases):
    """text에서 매칭되는 키워드 리스트 반환 (canonical name)"""
    text_lower = text.lower()
    matched = []
    for kw in keywords:
        patterns = [kw.lower()] + [a.lower() for a in aliases.get(kw, [])]
        if any(p in text_lower for p in patterns):
            matched.append(kw)
    return matched


def classify_and_tag(posts, cfg):
    """1차 키워드로 필터링, 2차 키워드로 태그 부여"""
    primary_kws = cfg["primary_keywords"]
    secondary_kws = cfg["secondary_keywords"]
    primary_aliases = cfg.get("primary_aliases", {})
    secondary_aliases = cfg.get("secondary_aliases", {})

    result = []
    for p in posts:
        p1 = match_keywords(p["text"], primary_kws, primary_aliases)
        if not p1:
            continue   # 1차 키워드 매칭 없으면 드랍
        tags = match_keywords(p["text"], secondary_kws, secondary_aliases)
        if not tags:
            continue   # 2차 태그 없으면 드랍 (대시보드에서 쓸 수 없음)

        p["primary"] = p1[0]   # 대표 1차 키워드 하나만 (가장 먼저 매칭된 것)
        p["tags"] = tags
        result.append(p)

    return result


# =====================================================
# Claude API 감성분석 (없으면 룰 기반 폴백)
# =====================================================
RULE_POS = ["좋다", "좋네", "좋음", "굿", "재밌", "최고", "만족", "감사", "잘만", "멋짐", "good", "great", "love", "amazing", "awesome", "best", "nice", "fun"]
RULE_NEG = ["별로", "싫다", "싫어", "쓰레기", "망함", "노잼", "화남", "짜증", "답없", "ㅡㅡ", "bad", "awful", "terrible", "worst", "useless", "broken", "nerf", "너프"]
RULE_IMP = ["했으면", "필요", "요청", "제안", "개선", "바람", "바랍", "원함", "원해", "부탁", "need", "should", "could", "suggest", "request", "improve", "would love", "wish"]


def sentiment_rule(text):
    t = text.lower()
    pos = sum(1 for w in RULE_POS if w in t)
    neg = sum(1 for w in RULE_NEG if w in t)
    imp = sum(1 for w in RULE_IMP if w in t)
    if imp > max(pos, neg) or (imp >= 1 and "?" in text):
        return "개선"
    if neg > pos:
        return "부정"
    if pos > 0:
        return "긍정"
    return "개선" if imp else "긍정"


def sentiment_batch_ai(posts, cfg):
    """Claude에게 배치로 감성 분석 요청"""
    if not HAS_ANTHROPIC or not cfg.get("use_ai_sentiment"):
        return [sentiment_rule(p["text"]) for p in posts]
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("[ai] ANTHROPIC_API_KEY 환경변수 없음, 룰 기반으로 폴백")
        return [sentiment_rule(p["text"]) for p in posts]

    client = Anthropic()
    model = cfg.get("claude_model", "claude-opus-4-7")
    results = []

    # 배치 크기 8
    BATCH = 8
    for i in range(0, len(posts), BATCH):
        chunk = posts[i:i + BATCH]
        numbered = "\n\n".join([f"[{j+1}] {p['text'][:500]}" for j, p in enumerate(chunk)])
        prompt = f"""게임 커뮤니티 게시글 {len(chunk)}건의 감성을 분류해주세요.

각 게시글은 다음 3가지 중 하나:
- 긍정: 칭찬, 만족, 즐거움
- 부정: 불만, 비판, 비난
- 개선: 요청사항, 제안, 아이디어 (부정보다 건설적인 톤)

게시글:
{numbered}

응답은 반드시 아래 JSON 배열 형식으로만 출력하세요. 설명 없이 JSON만.
["긍정","부정",...]"""

        try:
            msg = client.messages.create(
                model=model,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text.strip()
            # JSON 배열 추출
            m = re.search(r"\[.*\]", text, re.DOTALL)
            if not m:
                raise ValueError("JSON 배열 없음")
            arr = json.loads(m.group())
            if len(arr) != len(chunk):
                raise ValueError(f"개수 불일치: {len(arr)} vs {len(chunk)}")
            results.extend(arr)
            print(f"[ai] {i+len(chunk)}/{len(posts)} 분석 완료")
        except Exception as e:
            print(f"[ai] 배치 {i} 실패, 룰 기반 폴백: {e}")
            results.extend([sentiment_rule(p["text"]) for p in chunk])
        time.sleep(0.5)

    return results


# =====================================================
# 메인 파이프라인
# =====================================================
def main():
    cfg = load_or_create_config()
    print("=" * 60)
    print(f"TFD Insight Crawler")
    print(f"기간: Reddit {cfg['reddit']['date_start']} ~ {cfg['reddit']['date_end']}")
    print(f"       DC    {cfg['dc']['date_start']} ~ {cfg['dc']['date_end']}")
    print(f"1차: {cfg['primary_keywords']}")
    print(f"2차: {cfg['secondary_keywords']}")
    print("=" * 60)

    all_posts = []
    all_posts.extend(crawl_reddit(cfg["reddit"]))
    all_posts.extend(crawl_dc(cfg["dc"]))
    print(f"\n총 수집: {len(all_posts)}건")

    if not all_posts:
        print("수집된 게시글이 없습니다. 설정을 확인해주세요.")
        return

    # 키워드 필터 + 태깅
    classified = classify_and_tag(all_posts, cfg)
    print(f"키워드 매칭 후: {len(classified)}건")

    if not classified:
        print("키워드 매칭 결과가 없습니다. primary/secondary 키워드 또는 aliases를 확인해주세요.")
        return

    # 감성 분석
    print(f"\n감성 분석 시작 ({'Claude AI' if cfg.get('use_ai_sentiment') and HAS_ANTHROPIC else '룰 기반'})...")
    sentiments = sentiment_batch_ai(classified, cfg)
    for p, s in zip(classified, sentiments):
        p["sentiment"] = s if s in ("긍정", "부정", "개선") else "개선"

    # id 부여
    for i, p in enumerate(classified):
        p["id"] = i + 1

    # 저장
    out_path = cfg.get("output_path", "data.json")
    Path(out_path).write_text(
        json.dumps(classified, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # 통계
    sent_count = {s: sum(1 for p in classified if p["sentiment"] == s) for s in ("긍정", "부정", "개선")}
    src_count = {s: sum(1 for p in classified if p["source"] == s) for s in ("Reddit", "DC갤러리")}

    print("\n" + "=" * 60)
    print(f"저장 완료: {out_path} ({len(classified)}건)")
    print(f"소스: Reddit {src_count['Reddit']}건 · DC갤러리 {src_count['DC갤러리']}건")
    print(f"감성: 긍정 {sent_count['긍정']}건 · 부정 {sent_count['부정']}건 · 개선 {sent_count['개선']}건")
    print(f"\n다음: TFD Insight HTML 열어서 'Upload JSON' 버튼으로 {out_path} 업로드")
    print("=" * 60)


if __name__ == "__main__":
    main()
