
import os
import re
import csv
import json
import time
import random
import traceback
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright

# =========================================================
# 基础配置
# =========================================================
COOKIES_FILE = "./ks_cookies/1_zj.txt"
OUTPUT_ROOT = "/data3/jingzhang/program2/recommend_data/"
RECO_URL = "https://www.kuaishou.com/new-reco"
STATE_DIR = os.path.join(OUTPUT_ROOT, "_state")
VIDEO_DIR = os.path.join(OUTPUT_ROOT, "videos")
CSV_PATH = os.path.join(OUTPUT_ROOT, "reco_accounts_and_videos.csv")
SEEN_VIDEO_IDS_PATH = os.path.join(STATE_DIR, "seen_video_ids.json")

MAX_RECO_VIDEOS = 1000
MAX_SCROLL_COUNT = 3000
HTTP_TIMEOUT = 30
HTTP_RETRIES = 3
CHUNK_SIZE = 1024 * 256
HEADLESS = True
DEBUG = True
WAIT_AFTER_NAV = (3, 5)
WAIT_AFTER_NEXT = (2, 4)

illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n', '\r']


# =========================================================
# 工具函数
# =========================================================
def debug_print(msg):
    if DEBUG:
        print(msg)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def jitter(delay_range):
    time.sleep(random.uniform(*delay_range))


def safe_filename(name, max_len=80):
    if not name:
        name = "无标题"
    for ch in illegal_chars:
        name = name.replace(ch, "")
    name = " ".join(name.strip().split())
    return (name or "无标题")[:max_len]


def load_json_set(path):
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data if x)
    except Exception:
        pass
    return set()


def save_json_set(path, values):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(list(values)), f, ensure_ascii=False, indent=2)


def append_csv_row(row, csv_path=CSV_PATH):
    ensure_dir(os.path.dirname(csv_path))
    fields = [
        "user_id", "user_name", "profile_url", "intro", "is_living",
        "video_id", "video_title", "video_url", "cover_url", "duration_ms",
        "caption", "source_page", "saved_path"
    ]
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fields})


def load_cookies_and_headers(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    data = {}
    exec(content, {}, data)

    cookies = data.get("cookies", {})
    headers = data.get("headers", {})

    if not isinstance(cookies, dict):
        raise ValueError(f"cookie 文件无效：{file_path}")
    if not isinstance(headers, dict):
        raise ValueError(f"headers 文件无效：{file_path}")

    headers.setdefault(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145 Safari/537.36"
    )
    headers.setdefault("Referer", "https://www.kuaishou.com/")
    headers.setdefault("Origin", "https://www.kuaishou.com")
    headers.setdefault("Accept", "application/json, text/plain, */*")
    return cookies, headers


def build_download_session(headers, cookies):
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(headers)
    session.cookies.update(cookies)
    return session


# =========================================================
# 下载
# =========================================================
def guess_ext_from_url(url):
    path = urlparse(url).path.lower()
    if path.endswith(".mp4"):
        return ".mp4"
    if path.endswith(".mov"):
        return ".mov"
    if path.endswith(".mkv"):
        return ".mkv"
    return ".mp4"


def download_video(session, row, index):
    user_name = safe_filename(row.get("user_name", "未知账号"), max_len=40)
    user_id = str(row.get("user_id") or "unknown")
    video_id = str(row.get("video_id") or f"video_{index}")
    video_title = safe_filename(row.get("video_title") or row.get("caption") or f"video_{video_id}", max_len=50)

    author_dir = os.path.join(VIDEO_DIR, f"{user_name}_{user_id}")
    ensure_dir(author_dir)

    ext = guess_ext_from_url(row["video_url"])
    filename = f"{index:05d}_{video_id}_{video_title}{ext}"
    filepath = os.path.join(author_dir, filename)
    tmp_path = filepath + ".part"

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        return filepath, "skip"

    with session.get(
        row["video_url"],
        stream=True,
        timeout=HTTP_TIMEOUT,
        headers={"Referer": "https://www.kuaishou.com/"}
    ) as resp:
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    os.replace(tmp_path, filepath)
    return filepath, "downloaded"


# =========================================================
# 数据抽取：网络响应
# =========================================================
PROFILE_PATTERNS = [
    re.compile(r"https?://www\.kuaishou\.com/profile/([^/?#]+)"),
    re.compile(r"/profile/([^/?#]+)"),
]


def normalize_profile_url(user_id=None, profile_url=None):
    if profile_url:
        if profile_url.startswith("/"):
            return urljoin("https://www.kuaishou.com", profile_url)
        return profile_url
    if user_id:
        return f"https://www.kuaishou.com/profile/{user_id}"
    return ""


def normalize_bool_living(value):
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    if isinstance(value, dict):
        for k in ["living", "isLiving", "live", "isLive", "inLive"]:
            if k in value:
                return normalize_bool_living(value.get(k))
    return 0


def deep_find_dicts(obj, wanted_keys=None):
    result = []
    if isinstance(obj, dict):
        if wanted_keys is None or any(k in obj for k in wanted_keys):
            result.append(obj)
        for v in obj.values():
            result.extend(deep_find_dicts(v, wanted_keys))
    elif isinstance(obj, list):
        for item in obj:
            result.extend(deep_find_dicts(item, wanted_keys))
    return result


def extract_user_from_any(author_like):
    if not isinstance(author_like, dict):
        return None

    user_id = (
        author_like.get("id")
        or author_like.get("user_id")
        or author_like.get("userId")
        or author_like.get("authorId")
        or author_like.get("principalId")
    )
    user_name = (
        author_like.get("name")
        or author_like.get("user_name")
        or author_like.get("userName")
        or author_like.get("authorName")
        or author_like.get("nickname")
        or ""
    )
    intro = (
        author_like.get("user_text")
        or author_like.get("description")
        or author_like.get("intro")
        or author_like.get("brief")
        or author_like.get("signature")
        or ""
    )
    profile_url = (
        author_like.get("profileUrl")
        or author_like.get("profile_url")
        or author_like.get("userProfileUrl")
        or author_like.get("homeUrl")
        or author_like.get("url")
        or ""
    )
    is_living = normalize_bool_living(
        author_like.get("livingInfo")
        or author_like.get("living")
        or author_like.get("isLiving")
        or author_like.get("liveStatus")
    )

    if not user_id and profile_url:
        for pat in PROFILE_PATTERNS:
            m = pat.search(profile_url)
            if m:
                user_id = m.group(1)
                break

    if not user_id and not user_name:
        return None

    return {
        "user_id": str(user_id or ""),
        "user_name": user_name or "未知",
        "profile_url": normalize_profile_url(user_id, profile_url),
        "intro": intro or "",
        "is_living": is_living,
    }


def choose_video_url(photo_like):
    if not isinstance(photo_like, dict):
        return ""
    main_mv_urls = photo_like.get("mainMvUrls")
    return (
        photo_like.get("photoUrl")
        or photo_like.get("photoH265Url")
        or photo_like.get("playUrl")
        or (main_mv_urls[0].get("url") if isinstance(main_mv_urls, list) and main_mv_urls else "")
        or ""
    )


def choose_cover_url(photo_like):
    if not isinstance(photo_like, dict):
        return ""
    cover_urls = photo_like.get("coverUrls")
    return (
        photo_like.get("coverUrl")
        or (cover_urls[0].get("url") if isinstance(cover_urls, list) and cover_urls else "")
        or photo_like.get("poster")
        or ""
    )


def extract_video_from_feed(feed_like):
    if not isinstance(feed_like, dict):
        return None
    photo = feed_like.get("photo") if isinstance(feed_like.get("photo"), dict) else feed_like
    video_id = photo.get("id") or photo.get("photoId") or photo.get("videoId") or feed_like.get("id")
    video_url = choose_video_url(photo)
    if not video_id or not video_url:
        return None
    caption = photo.get("caption") or photo.get("originCaption") or photo.get("title") or ""
    return {
        "video_id": str(video_id),
        "video_title": caption or f"video_{video_id}",
        "video_url": video_url,
        "cover_url": choose_cover_url(photo),
        "duration_ms": photo.get("duration", 0),
        "caption": caption,
    }


def extract_pairs_from_json(data_obj):
    pairs = []
    feed_dicts = deep_find_dicts(data_obj, wanted_keys={"photo", "author"})
    for fd in feed_dicts:
        user = extract_user_from_any(fd.get("author"))
        video = extract_video_from_feed(fd)
        if user and video:
            pairs.append({**user, **video})

    all_author_like = deep_find_dicts(data_obj, wanted_keys={"name", "user_name", "nickname", "livingInfo", "profileUrl"})
    all_photo_like = deep_find_dicts(data_obj, wanted_keys={"photoUrl", "photoH265Url", "coverUrl", "duration", "caption"})
    authors = [extract_user_from_any(x) for x in all_author_like]
    authors = [x for x in authors if x]
    videos = [extract_video_from_feed(x) for x in all_photo_like]
    videos = [x for x in videos if x]

    if not pairs and authors and videos:
        pairs.append({**authors[0], **videos[0]})

    uniq = []
    seen = set()
    for item in pairs:
        key = (item.get("user_id", ""), item.get("video_id", ""))
        if key not in seen and item.get("video_url"):
            seen.add(key)
            uniq.append(item)
    return uniq


# =========================================================
# 数据抽取：当前 DOM
# =========================================================
def extract_current_reco_from_dom(page):
    return page.evaluate(
        """
        () => {
          function textClean(s) {
            return (s || '').replace(/\s+/g, ' ').trim();
          }

          function visible(el) {
            if (!el) return false;
            const rect = el.getBoundingClientRect();
            const style = window.getComputedStyle(el);
            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
          }

          function inViewportScore(el) {
            const r = el.getBoundingClientRect();
            const vh = window.innerHeight || document.documentElement.clientHeight;
            const vw = window.innerWidth || document.documentElement.clientWidth;
            const cx = Math.max(0, Math.min(r.right, vw) - Math.max(r.left, 0));
            const cy = Math.max(0, Math.min(r.bottom, vh) - Math.max(r.top, 0));
            return cx * cy;
          }

          const videos = Array.from(document.querySelectorAll('video')).filter(visible);
          if (!videos.length) return null;

          videos.sort((a, b) => inViewportScore(b) - inViewportScore(a));
          const v = videos[0];
          const rect = v.getBoundingClientRect();
          const centerX = rect.left + rect.width / 2;
          const centerY = rect.top + rect.height / 2;

          let container = v.closest('div, section, article') || v.parentElement || document.body;
          let up = container;
          for (let i = 0; i < 6 && up && up.parentElement; i++) {
            up = up.parentElement;
            if (up.querySelector && up.querySelector('a[href*="/profile/"]')) {
              container = up;
              break;
            }
          }

          const allProfileAnchors = Array.from(document.querySelectorAll('a[href*="/profile/"]')).filter(visible);
          let anchor = null;
          if (container) {
            anchor = container.querySelector('a[href*="/profile/"]');
          }
          if (!anchor && allProfileAnchors.length) {
            allProfileAnchors.sort((a, b) => {
              const ar = a.getBoundingClientRect();
              const br = b.getBoundingClientRect();
              const ad = Math.abs(ar.left + ar.width / 2 - centerX) + Math.abs(ar.top + ar.height / 2 - centerY);
              const bd = Math.abs(br.left + br.width / 2 - centerX) + Math.abs(br.top + br.height / 2 - centerY);
              return ad - bd;
            });
            anchor = allProfileAnchors[0];
          }

          const profileUrl = anchor ? anchor.href : '';
          const userName = anchor ? textClean(anchor.textContent) : '';

          let intro = '';
          if (container) {
            const textNodes = Array.from(container.querySelectorAll('span, div, p')).map(el => textClean(el.textContent)).filter(Boolean);
            const uniq = [];
            const seen = new Set();
            for (const t of textNodes) {
              if (!seen.has(t) && t.length >= 2 && t.length <= 120) {
                seen.add(t);
                uniq.push(t);
              }
            }
            const filtered = uniq.filter(t => t !== userName && !t.includes('关注') && !t.includes('粉丝') && !t.includes('赞'));
            intro = filtered.slice(0, 3).join(' | ');
          }

          let videoUrl = v.currentSrc || v.src || '';
          if (!videoUrl) {
            const source = v.querySelector('source[src]');
            if (source) videoUrl = source.src || '';
          }

          let poster = v.poster || '';
          const titleEl = container ? container.querySelector('h1, h2, h3, [title], .title, .caption') : null;
          const title = titleEl ? (titleEl.getAttribute('title') || textClean(titleEl.textContent)) : '';

          return {
            profile_url: profileUrl,
            user_name: userName,
            intro: intro,
            is_living: 0,
            video_url: videoUrl,
            cover_url: poster,
            duration_ms: Math.round((v.duration && isFinite(v.duration) ? v.duration * 1000 : 0)),
            caption: title,
            video_title: title,
            page_url: location.href,
            page_text: textClean((container ? container.innerText : document.body.innerText) || '').slice(0, 1000),
          };
        }
        """
    )


def enrich_dom_row(row):
    if not row:
        return None
    profile_url = row.get("profile_url", "") or ""
    user_id = ""
    if profile_url:
        for pat in PROFILE_PATTERNS:
            m = pat.search(profile_url)
            if m:
                user_id = m.group(1)
                break

    video_url = row.get("video_url", "") or ""
    video_id = ""
    if video_url:
        m = re.search(r"/([^/?#]+)\.(mp4|mov|mkv)(?:\?|$)", video_url, flags=re.I)
        if m:
            video_id = m.group(1)
    if not video_id:
        m = re.search(r"photoId[=:]([^&\"'\s]+)", video_url)
        if m:
            video_id = m.group(1)

    if not video_id and profile_url and row.get("caption"):
        raw = f"{profile_url}|{row.get('caption')}|{video_url[:80]}"
        video_id = str(abs(hash(raw)))

    return {
        "user_id": str(user_id or ""),
        "user_name": row.get("user_name", "") or "未知",
        "profile_url": normalize_profile_url(user_id, profile_url),
        "intro": row.get("intro", "") or "",
        "is_living": row.get("is_living", 0) or 0,
        "video_id": str(video_id or ""),
        "video_title": row.get("video_title", "") or row.get("caption", "") or "无标题",
        "video_url": video_url,
        "cover_url": row.get("cover_url", "") or "",
        "duration_ms": row.get("duration_ms", 0) or 0,
        "caption": row.get("caption", "") or "",
    }


# =========================================================
# 页面翻到下一个推荐
# =========================================================
def go_next_reco(page):
    actions = [
        lambda: page.keyboard.press("PageDown"),
        lambda: page.keyboard.press("ArrowDown"),
        lambda: page.mouse.wheel(0, 1200),
        lambda: page.locator("body").click(position={"x": 720, "y": 420}),
        lambda: page.keyboard.press("Space"),
    ]
    for action in actions:
        try:
            action()
            return True
        except Exception:
            continue
    try:
        page.evaluate("window.scrollBy(0, window.innerHeight)")
        return True
    except Exception:
        return False


# =========================================================
# 主流程
# =========================================================
def scrape_kuaishou_reco():
    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STATE_DIR)
    ensure_dir(VIDEO_DIR)

    seen_video_ids = load_json_set(SEEN_VIDEO_IDS_PATH)
    cookies, headers = load_cookies_and_headers(COOKIES_FILE)
    session = build_download_session(headers, cookies)

    total_saved = 0
    parsed_cache = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(
            user_agent=headers.get("User-Agent"),
            viewport={"width": 1440, "height": 900},
            extra_http_headers={
                "Referer": "https://www.kuaishou.com/",
                "Origin": "https://www.kuaishou.com",
            }
        )

        browser_cookies = []
        for name, value in cookies.items():
            browser_cookies.append({
                "name": str(name),
                "value": str(value),
                "domain": ".kuaishou.com",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "Lax",
            })
        if browser_cookies:
            context.add_cookies(browser_cookies)

        page = context.new_page()

        def on_response(resp):
            nonlocal parsed_cache
            try:
                ctype = (resp.headers or {}).get("content-type", "")
                if "json" not in ctype and "javascript" not in ctype and "text/plain" not in ctype:
                    return
                text = resp.text()
                if not text or len(text) < 20:
                    return
                body = text[:6000].lower()
                url = (resp.url or "").lower()
                if not any(k in url or k in body for k in ["photo", "coverurl", "graphql", "feed", "vision", "profile", "author"]):
                    return
                try:
                    obj = json.loads(text)
                except Exception:
                    return
                pairs = extract_pairs_from_json(obj)
                if pairs:
                    parsed_cache = (pairs + parsed_cache)[:50]
                    debug_print(f"[监听到接口数据] 新增候选 {len(pairs)} 条")
            except Exception:
                pass

        page.on("response", on_response)

        print("=" * 60)
        print("打开快手推荐页")
        print("=" * 60)
        page.goto(RECO_URL, wait_until="domcontentloaded", timeout=60000)
        jitter(WAIT_AFTER_NAV)

        last_video_key = None
        stagnant_rounds = 0

        for loop_idx in range(1, MAX_SCROLL_COUNT + 1):
            if total_saved >= MAX_RECO_VIDEOS:
                break

            print(f"\n[循环 {loop_idx}] 当前已保存 {total_saved}/{MAX_RECO_VIDEOS}")

            row = None
            try:
                dom_row = extract_current_reco_from_dom(page)
                row = enrich_dom_row(dom_row)
                debug_print(f"[DOM抓取] user={row.get('user_name')} profile={row.get('profile_url')} video={row.get('video_url')[:120] if row.get('video_url') else ''}")
            except Exception as e:
                debug_print(f"[DOM抓取异常] {e}")

            if row and row.get("video_url"):
                if parsed_cache:
                    for item in parsed_cache:
                        if row.get("video_id") and item.get("video_id") == row.get("video_id"):
                            row = {**item, **row}
                            break
                        if row.get("video_url") and item.get("video_url") == row.get("video_url"):
                            row = {**item, **row}
                            break
                        if row.get("profile_url") and item.get("profile_url") == row.get("profile_url"):
                            row = {**item, **row}
                            break

                video_id = str(row.get("video_id") or "")
                video_key = video_id or row.get("video_url") or ""

                if video_key == last_video_key:
                    stagnant_rounds += 1
                else:
                    stagnant_rounds = 0
                last_video_key = video_key

                if video_id and video_id not in seen_video_ids:
                    try:
                        save_path, status = download_video(session, row, total_saved + 1)
                        row["saved_path"] = save_path
                        row["source_page"] = RECO_URL
                        append_csv_row(row)
                        seen_video_ids.add(video_id)
                        save_json_set(SEEN_VIDEO_IDS_PATH, seen_video_ids)
                        total_saved += 1
                        print(f"[保存成功] {status} | {row.get('user_name')} | video_id={video_id}")
                    except Exception as e:
                        print(f"[下载失败] {row.get('user_name')} | video_id={video_id} | err={e}")
                elif not video_id:
                    debug_print("[跳过] 当前视频缺少 video_id")
                else:
                    debug_print(f"[跳过] 已抓过 video_id={video_id}")
            else:
                stagnant_rounds += 1
                debug_print("[未获取到当前视频URL] 本轮只翻页")

            go_next_reco(page)
            jitter(WAIT_AFTER_NEXT)

            if stagnant_rounds >= 5:
                print("[提示] 连续多轮停留在同一视频或未提取到新视频，尝试额外滚动和点击")
                try:
                    page.locator("body").click(position={"x": 720, "y": 420}, timeout=1500)
                except Exception:
                    pass
                try:
                    page.mouse.wheel(0, 1800)
                except Exception:
                    pass
                jitter((2, 3))
                stagnant_rounds = 0

        browser.close()

    print("\n全部完成")
    print(f"共下载视频数：{len(seen_video_ids)}")
    print(f"CSV：{CSV_PATH}")


if __name__ == "__main__":
    try:
        scrape_kuaishou_reco()
    except Exception as e:
        print(f"[主流程异常] {e}")
        print(traceback.format_exc())
