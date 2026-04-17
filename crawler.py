"""
TFD Insight Crawler
====================
Reddit + DC갤러리 게시글을 크롤링해서 룰 기반 감성 분석 후 JSON 출력.

사용법:
    1) 의존성 설치
       pip install requests beautifulsoup4

    2) config.json 편집 후 실행
       python crawler.py

    3) 생성된 data.json을 TFD Insight HTML의 Upload JSON 버튼에 올리기
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
    "categories": [
        {
            "name": "타워디펜스",
            "keywords": [
                {"value": "포탑", "aliases": ["turret", "터렛"]},
                {"value": "바리케이트", "aliases": ["barricade", "바리케이드", "벽"]},
                {"value": "웨이브", "aliases": ["wave"]},
            ],
        },
        {
            "name": "밸런스",
            "keywords": [
                {"value": "스킬", "aliases": ["skill", "ability"]},
                {"value": "캐릭터", "aliases": ["character", "descendant", "계승자"]},
            ],
        },
        {
            "name": "신규컨텐츠",
            "keywords": [
                {"value": "던전", "aliases": ["dungeon", "레이드"]},
                {"value": "보상", "aliases": ["reward", "drop", "loot", "드랍", "드롭"]},
            ],
        },
    ],
    "output_path": "data.json",
    "translate_english": True,  # 영어 게시글 한국어 자동 번역
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
# 번역 (MyMemory 무료 API, 키 불필요)
# =====================================================
def translate_to_korean(text, max_chars=500):
    """영어 텍스트를 한국어로 번역. MyMemory 무료 API 사용.
    실패 시 None 반환. 긴 텍스트는 max_chars로 잘라서 번역."""
    if not text or not text.strip():
        return None
    # 너무 긴 텍스트는 잘라서 번역 (API limit 500자)
    snippet = text[:max_chars]
    try:
        r = requests.get(
            "https://api.mymemory.translated.net/get",
            params={"q": snippet, "langpair": "en|ko"},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        translated = data.get("responseData", {}).get("translatedText", "")
        # API가 에러 메시지를 번역문으로 반환하는 경우 필터
        if "MYMEMORY WARNING" in translated.upper() or "INVALID" in translated.upper():
            return None
        return translated.strip() or None
    except Exception as e:
        print(f"[translate] 실패: {e}")
        return None


def is_mostly_english(text):
    """텍스트가 주로 영어인지 간이 판정 (한글 비율이 10% 미만이면 영어로 간주)"""
    if not text:
        return False
    korean_chars = sum(1 for c in text if '\uac00' <= c <= '\ud7a3')
    total_chars = sum(1 for c in text if c.isalpha())
    if total_chars == 0:
        return False
    return (korean_chars / total_chars) < 0.1


def translate_posts(posts):
    """영어 게시글에 한국어 번역 추가 (text_ko 필드)"""
    english_posts = [p for p in posts if is_mostly_english(p.get("text", ""))]
    if not english_posts:
        return posts

    print(f"[translate] 영어 게시글 {len(english_posts)}건 번역 중...")
    for i, p in enumerate(english_posts):
        translated = translate_to_korean(p["text"])
        if translated:
            p["text_ko"] = translated
        # API rate limit 방지 (초당 1회)
        if i < len(english_posts) - 1:
            time.sleep(0.6)
        if (i + 1) % 10 == 0:
            print(f"[translate] {i+1}/{len(english_posts)} 완료")
    print(f"[translate] 번역 완료")
    return posts



# =====================================================
# DC갤러리 크롤러 (PC 버전, 마이너 갤러리 지원)
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://gall.dcinside.com/",
    })

    for page in range(1, pages + 1):
        # PC 버전 마이너 갤러리 URL
        list_url = f"https://gall.dcinside.com/mgallery/board/lists/?id={gall_id}&page={page}"
        try:
            r = session.get(list_url, timeout=15)
            if r.status_code != 200:
                print(f"[dc] page {page} 응답 {r.status_code}, 중단")
                break
        except Exception as e:
            print(f"[dc] page {page} 요청 실패: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # 게시글 목록: <tr class="ub-content us-post"> 안의 각 행
        rows = soup.select("tr.ub-content.us-post")
        if not rows:
            print(f"[dc] page {page}: 게시글 행 없음 (셀렉터 불일치 가능)")
            break

        found_in_page = 0
        for row in rows:
            try:
                # 글 유형 (공지/설문/AD 등은 건너뛰기)
                subject_el = row.select_one("td.gall_subject")
                if subject_el:
                    subject_text = subject_el.get_text(strip=True)
                    if subject_text in ("공지", "설문", "AD"):
                        continue

                # 제목 추출
                title_el = row.select_one("td.gall_tit a")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not title:
                    continue

                # URL 추출
                href = title_el.get("href", "")
                if not href.startswith("http"):
                    href = "https://gall.dcinside.com" + href

                # 글 번호 (게시글 고유 번호)
                num_el = row.select_one("td.gall_num")
                post_num = num_el.get_text(strip=True) if num_el else ""

                # 날짜 추출
                date_el = row.select_one("td.gall_date")
                date_str = date_el.get("title", "") or date_el.get_text(strip=True) if date_el else ""
                post_date = parse_dc_date(date_str)

                # 날짜 필터링 (날짜 파싱 실패하면 오늘 날짜로 대체)
                if post_date is None:
                    post_date = datetime.now().date()
                if not (date_start <= post_date <= date_end):
                    continue

                # 추천수
                rec_el = row.select_one("td.gall_recommend")
                upvotes = 0
                if rec_el:
                    try:
                        upvotes = int(rec_el.get_text(strip=True) or 0)
                    except ValueError:
                        upvotes = 0

                # 조회수
                count_el = row.select_one("td.gall_count")
                views = 0
                if count_el:
                    try:
                        views = int(count_el.get_text(strip=True) or 0)
                    except ValueError:
                        views = 0

                posts.append({
                    "source": "DC갤러리",
                    "text": title,          # 리스트에서는 제목만 수집 (본문 접근은 추가 요청 필요)
                    "date": post_date.strftime("%Y-%m-%d"),
                    "upvotes": upvotes,
                    "views": views,
                    "url": href,
                })
                found_in_page += 1

            except Exception as e:
                continue

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
# 키워드 매칭 (카테고리 그룹 기반 OR 매칭)
# =====================================================
def match_keyword(text_lower, value, aliases):
    """단일 키워드가 텍스트에 매칭되는지 확인"""
    patterns = [value.lower()] + [a.lower() for a in aliases]
    return any(p in text_lower for p in patterns)


def classify_and_tag(posts, cfg):
    """카테고리 구조 기반 분류:
    - 각 게시글에서 활성화된 2차 키워드를 찾는다
    - 첫 매칭 카테고리를 primary로 지정 (중복 방지)
    - 같은 카테고리 내 모든 매칭 키워드를 tags에 저장

    입력 cfg 형식:
        {
            "categories": [
                {"name": "프로토스", "keywords": [{"value": "질럿", "aliases": [...]}, ...]},
                ...
            ]
        }
    구버전 호환: primary_keywords/secondary_keywords가 있으면 categories로 변환
    """
    # 구버전 호환 처리 (primary_keywords + secondary_keywords → categories)
    if "categories" not in cfg or not cfg["categories"]:
        categories = _legacy_to_categories(cfg)
    else:
        categories = cfg["categories"]

    if not categories:
        return []

    result = []
    for p in posts:
        text_lower = p["text"].lower()

        # 각 카테고리를 순회하며 매칭되는 첫 카테고리 찾기
        matched_category = None
        matched_tags = []

        for cat in categories:
            cat_name = cat["name"]
            keywords = cat.get("keywords", [])

            # 이 카테고리에서 매칭되는 모든 키워드 수집
            tags_in_cat = []
            for kw in keywords:
                # kw가 문자열이면 {"value": kw, "aliases": []}로 처리
                if isinstance(kw, str):
                    value, aliases = kw, []
                else:
                    value = kw.get("value", "")
                    aliases = kw.get("aliases", [])

                if value and match_keyword(text_lower, value, aliases):
                    tags_in_cat.append(value)

            # 이 카테고리에서 매칭된 키워드가 있으면 이 카테고리로 확정
            if tags_in_cat:
                matched_category = cat_name
                matched_tags = tags_in_cat
                break   # 첫 매칭 카테고리만 사용 (중복 방지)

        if matched_category is None:
            continue   # 어떤 카테고리에도 매칭되지 않으면 드랍

        p["primary"] = matched_category
        p["tags"] = matched_tags
        result.append(p)

    return result


def _legacy_to_categories(cfg):
    """구버전 flat 키워드 구조를 카테고리 구조로 변환 (백엔드 단독 실행 호환용)"""
    primary_kws = cfg.get("primary_keywords", [])
    secondary_kws = cfg.get("secondary_keywords", [])
    secondary_aliases = cfg.get("secondary_aliases", {})

    if not primary_kws or not secondary_kws:
        return []

    return [{
        "name": primary_kws[0],
        "keywords": [
            {"value": kw, "aliases": secondary_aliases.get(kw, [])}
            for kw in secondary_kws
        ],
    }]


# =====================================================
# 키워드 발견 (게시글에서 자주 나온 단어 추출)
# =====================================================

STOPWORDS_KO = set("이 가 은 는 을 를 의 에 에서 으로 로 와 과 도 만 까지 부터 보다 처럼 같이 것 수 등 중 때 위 후 뒤 더 또 및 그 저 이런 저런 그런 합니다 한다 하다 있다 없다 되다 않다 이다 해서 하고 해요 입니다 ㅋㅋ ㅎㅎ ㅋㅋㅋ ㅎㅎㅎ ㅋㅋㅋㅋ ㅠㅠ ㅜㅜ ㄹㅇ ㅇㅇ ㄴㄴ ㅡㅡ 진짜 좀 너무 많이 다 안 못 왜 뭐 걍 근데 아 오 음 게임 하는 같은 있는 없는 되는 하는 해야 에서".split())
STOPWORDS_EN = set("the a an is are was were be been being have has had do does did will would shall should may might can could i me my we our you your he she it they them their its this that these those am not no nor so if or but and to of in for on at by from with as about into through during before after above below between out up down off over under again further then once here there when where why how all each every both few more most other some such only own same than too very just game games like really think make need want get got going been much also even still".split())


def extract_keywords_from_posts(posts, seed_keywords=None, top_n=40, min_length=2):
    """게시글에서 자주 등장하는 키워드를 빈도순으로 추출.

    Args:
        posts: 게시글 리스트 [{text, source, ...}, ...]
        seed_keywords: 시드(씨앗) 키워드 — 결과에서 제외 (이미 알고 있는 단어)
        top_n: 반환할 키워드 수
        min_length: 최소 글자 수

    Returns:
        [{"word": "포탑", "count": 47, "sources": {"Reddit": 30, "DC갤러리": 17}}, ...]
    """
    seed_set = set(w.lower() for w in (seed_keywords or []))
    word_data = {}

    for post in posts:
        text = post.get("text", "")
        source = post.get("source", "unknown")

        # 한글 2글자 이상 또는 영문 2글자 이상인 단어만 추출
        words_raw = re.findall(r'[가-힣]{2,}|[a-zA-Z]{2,}', text)

        # 한 게시글에서 같은 단어 여러 번 나와도 1회로 카운트
        seen_in_post = set()
        for w in words_raw:
            w_lower = w.lower()
            if len(w) < min_length:
                continue
            if w_lower in STOPWORDS_KO or w_lower in STOPWORDS_EN:
                continue
            if w_lower in seed_set:
                continue
            if w_lower in seen_in_post:
                continue
            seen_in_post.add(w_lower)

            if w_lower not in word_data:
                word_data[w_lower] = {"word": w, "count": 0, "sources": {}}
            word_data[w_lower]["count"] += 1
            word_data[w_lower]["sources"][source] = word_data[w_lower]["sources"].get(source, 0) + 1

    sorted_words = sorted(word_data.values(), key=lambda x: x["count"], reverse=True)
    return sorted_words[:top_n]


# =====================================================
# 룰 기반 감성분석 (한/영 감성 단어 사전)
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


def sentiment_batch(posts, cfg=None):
    """게시글 리스트에 대해 감성 분류 (룰 기반)"""
    return [sentiment_rule(p["text"]) for p in posts]


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

    # 감성 분석 (룰 기반)
    print(f"\n감성 분석 시작 (룰 기반)...")
    sentiments = sentiment_batch(classified, cfg)
    for p, s in zip(classified, sentiments):
        p["sentiment"] = s if s in ("긍정", "부정", "개선") else "개선"

    # 영어 게시글 번역 (Reddit → 한글)
    if cfg.get("translate_english", True):
        translate_posts(classified)

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
