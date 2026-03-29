import os
import re
import csv
import json
import time
import random
import traceback
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

# =========================================================
# 基础配置
# =========================================================
COOKIES_FILE = "./ks_cookies/1_zj.txt"
OUTPUT_ROOT = "/data3/jingzhang/program2/commend_data/"
STATE_DIR = os.path.join(OUTPUT_ROOT, "_state")
JSON_DIR = os.path.join(OUTPUT_ROOT, "json")
CSV_DIR = os.path.join(OUTPUT_ROOT, "csv")

HEADLESS = True
DEBUG = True
HTTP_TIMEOUT = 60000
WAIT_AFTER_NAV = (3, 5)
WAIT_AFTER_ACTION = (0.8, 1.6)
WAIT_AFTER_SCROLL = (1.0, 2.0)
MAX_EXPAND_ROUNDS = 80
MAX_REPLY_EXPAND_ROUNDS = 200
MAX_SCROLL_ROUNDS = 300
MAX_EMPTY_SCROLL_ROUNDS = 8

VIDEO_URL = "https://www.kuaishou.com/short-video/3xexample"  # 改成你要抓取的视频地址

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


def safe_filename(name, max_len=120):
    if not name:
        name = "无标题"
    for ch in illegal_chars:
        name = name.replace(ch, "")
    name = " ".join(name.strip().split())
    return (name or "无标题")[:max_len]


def parse_human_count(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip().replace(",", "")
    if not s:
        return 0
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)([万wW千kK]?)$", s)
    if m:
        num = float(m.group(1))
        unit = m.group(2)
        if unit in ("万", "w", "W"):
            return int(num * 10000)
        if unit in ("千", "k", "K"):
            return int(num * 1000)
        return int(num)
    digits = re.sub(r"[^0-9]", "", s)
    return int(digits) if digits else 0


def parse_video_id(video_url):
    if not video_url:
        return ""
    patterns = [
        r"/short-video/([^/?#]+)",
        r"/video/([^/?#]+)",
        r"photoId[=:]([^&?#\"'\s]+)",
        r"fid=([^&?#\"'\s]+)",
    ]
    for pat in patterns:
        m = re.search(pat, video_url)
        if m:
            return m.group(1)
    path = urlparse(video_url).path.strip("/")
    if path:
        return path.split("/")[-1]
    return ""


PROFILE_PATTERNS = [
    re.compile(r"https?://www\.kuaishou\.com/profile/([^/?#]+)"),
    re.compile(r"/profile/([^/?#]+)"),
]


def extract_user_id_from_profile(profile_url):
    if not profile_url:
        return ""
    for pat in PROFILE_PATTERNS:
        m = pat.search(profile_url)
        if m:
            return m.group(1)
    return ""


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


# =========================================================
# 评论抽取：JSON 响应
# =========================================================
def pick_first(obj, keys):
    if not isinstance(obj, dict):
        return None
    for k in keys:
        if k in obj and obj.get(k) not in (None, ""):
            return obj.get(k)
    return None


def extract_author_info(author_like):
    if not isinstance(author_like, dict):
        return {
            "user_name": "",
            "user_id": "",
            "profile_url": "",
        }

    user_name = pick_first(author_like, ["userName", "user_name", "name", "nickname", "authorName", "screenName"]) or ""
    user_id = pick_first(author_like, ["userId", "user_id", "id", "authorId", "principalId", "kwaiId"]) or ""
    profile_url = pick_first(author_like, ["profileUrl", "profile_url", "homeUrl", "url", "userProfileUrl"]) or ""

    if profile_url and profile_url.startswith("/"):
        profile_url = urljoin("https://www.kuaishou.com", profile_url)
    if not user_id and profile_url:
        user_id = extract_user_id_from_profile(profile_url)

    return {
        "user_name": str(user_name or ""),
        "user_id": str(user_id or ""),
        "profile_url": str(profile_url or ""),
    }


def normalize_comment_node(comment_like, parent_comment_id="", root_comment_id=""):
    if not isinstance(comment_like, dict):
        return None

    comment_id = pick_first(comment_like, ["commentId", "comment_id", "id", "cid"])
    text = pick_first(comment_like, ["content", "text", "commentContent", "comment", "body", "message"]) or ""
    like_count = pick_first(comment_like, ["likedCount", "likeCount", "like_count", "likes", "diggCount", "thumbCount"]) or 0
    create_time = pick_first(comment_like, ["timestamp", "time", "createTime", "createdAt", "commentTime"]) or ""

    author_like = (
        pick_first(comment_like, ["author", "user", "owner", "commentUser", "creator", "fromUser"])
        if isinstance(comment_like, dict) else None
    )
    author = extract_author_info(author_like if isinstance(author_like, dict) else {})

    # 有些结构会把用户信息铺平
    if not author["user_name"] and not author["user_id"]:
        author = extract_author_info(comment_like)

    if not comment_id and not text:
        return None

    current_id = str(comment_id or "")
    root_id = str(root_comment_id or current_id)
    parent_id = str(parent_comment_id or "")
    level = 1 if not parent_id else 2

    return {
        "comment_id": current_id,
        "parent_comment_id": parent_id,
        "root_comment_id": root_id,
        "level": level,
        "content": str(text).strip(),
        "like_count": parse_human_count(like_count),
        "create_time": create_time,
        "user_name": author.get("user_name", "") or "",
        "user_id": author.get("user_id", "") or "",
        "profile_url": author.get("profile_url", "") or "",
        "source": "network",
    }


def extract_nested_reply_candidates(comment_like):
    keys = [
        "subComments", "subComment", "replys", "replies", "replyList", "children",
        "commentReplies", "subCommentsV2", "hotReplies", "replyData"
    ]
    candidates = []
    if not isinstance(comment_like, dict):
        return candidates
    for k in keys:
        v = comment_like.get(k)
        if isinstance(v, list):
            candidates.extend(v)
        elif isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, list):
                    candidates.extend(vv)
    return candidates


def extract_comments_from_json(data_obj):
    results = []
    seen = set()

    candidate_dicts = deep_find_dicts(
        data_obj,
        wanted_keys={
            "commentId", "comment_id", "content", "commentContent", "likedCount",
            "likeCount", "subComments", "replys", "replyList", "rootCommentId"
        }
    )

    def add_comment_tree(comment_like, parent_comment_id="", root_comment_id=""):
        node = normalize_comment_node(comment_like, parent_comment_id=parent_comment_id, root_comment_id=root_comment_id)
        if node:
            key = (node["comment_id"], node["parent_comment_id"], node["content"])
            if key not in seen:
                seen.add(key)
                results.append(node)
            parent_id = node["comment_id"]
            root_id = node["root_comment_id"]
        else:
            parent_id = parent_comment_id
            root_id = root_comment_id

        for child in extract_nested_reply_candidates(comment_like):
            if isinstance(child, dict):
                add_comment_tree(child, parent_comment_id=parent_id, root_comment_id=root_id or parent_id)

    for item in candidate_dicts:
        add_comment_tree(item)

    # 有些接口会把主评论和回复拆成两个列表，补一遍 rootCommentId / parentCommentId
    extra_dicts = deep_find_dicts(
        data_obj,
        wanted_keys={"rootCommentId", "parentCommentId", "replyToCommentId", "commentId", "content"}
    )
    for item in extra_dicts:
        node = normalize_comment_node(
            item,
            parent_comment_id=str(
                pick_first(item, ["parentCommentId", "replyToCommentId", "parentId"]) or ""
            ),
            root_comment_id=str(
                pick_first(item, ["rootCommentId", "rootId"]) or pick_first(item, ["commentId", "comment_id", "id", "cid"]) or ""
            )
        )
        if node:
            if node["parent_comment_id"]:
                node["level"] = 2
            key = (node["comment_id"], node["parent_comment_id"], node["content"])
            if key not in seen:
                seen.add(key)
                results.append(node)

    return results


# =========================================================
# 评论抽取：DOM 兜底
# =========================================================
def extract_comments_from_dom(page):
    data = page.evaluate(
        r'''
        () => {
          function textClean(s) {
            return (s || '').replace(/\s+/g, ' ').trim();
          }
          function parseCount(s) {
            s = textClean(s).replace(/,/g, '');
            if (!s) return 0;
            const m = s.match(/^([0-9]+(?:\.[0-9]+)?)([万wW千kK]?)$/);
            if (m) {
              const num = parseFloat(m[1]);
              const unit = m[2];
              if (unit === '万' || unit === 'w' || unit === 'W') return Math.round(num * 10000);
              if (unit === '千' || unit === 'k' || unit === 'K') return Math.round(num * 1000);
              return Math.round(num);
            }
            const digits = s.replace(/[^0-9]/g, '');
            return digits ? parseInt(digits, 10) : 0;
          }
          function visible(el) {
            if (!el) return false;
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            return r.width > 0 && r.height > 0 && st.display !== 'none' && st.visibility !== 'hidden';
          }
          function bestText(el, selectors) {
            for (const sel of selectors) {
              const x = el.querySelector(sel);
              if (x && visible(x)) {
                const t = textClean(x.innerText || x.textContent || '');
                if (t) return t;
              }
            }
            return '';
          }
          function bestAttr(el, selectors, attr) {
            for (const sel of selectors) {
              const x = el.querySelector(sel);
              if (x && x.getAttribute(attr)) return x.getAttribute(attr);
            }
            return '';
          }
          function guessLike(el) {
            const texts = Array.from(el.querySelectorAll('span, div, p, button')).map(x => textClean(x.innerText || x.textContent || '')).filter(Boolean);
            for (const t of texts) {
              if (/^([0-9]+(?:\.[0-9]+)?)([万wW千kK]?)$/.test(t)) return parseCount(t);
            }
            return 0;
          }

          const commentBlocks = Array.from(document.querySelectorAll('div, li, section, article')).filter(visible).filter(el => {
            const txt = textClean(el.innerText || el.textContent || '');
            if (!txt || txt.length < 2) return false;
            const hasProfile = el.querySelector('a[href*="/profile/"]');
            const hasReply = /回复|楼中楼|展开|查看全部/.test(txt);
            return hasProfile || hasReply;
          });

          const out = [];
          const seen = new Set();
          for (const el of commentBlocks) {
            const profile = bestAttr(el, ['a[href*="/profile/"]'], 'href');
            const userName = bestText(el, ['a[href*="/profile/"]', '[class*="name"]', '[data-testid*="name"]']);
            const content = bestText(el, ['[class*="content"]', '[class*="text"]', 'p', 'span']);
            const like = guessLike(el);
            const commentId = el.getAttribute('data-id') || el.getAttribute('data-comment-id') || el.id || '';
            if (!content && !userName) continue;
            const key = [commentId, userName, content].join('||');
            if (seen.has(key)) continue;
            seen.add(key);
            out.push({
              comment_id: commentId,
              parent_comment_id: '',
              root_comment_id: commentId,
              level: 1,
              content,
              like_count: like,
              create_time: '',
              user_name: userName,
              user_id: '',
              profile_url: profile ? new URL(profile, location.origin).href : '',
              source: 'dom'
            });
          }
          return out;
        }
        '''
    )

    results = []
    for row in data or []:
        row = dict(row)
        if row.get("profile_url") and not row.get("user_id"):
            row["user_id"] = extract_user_id_from_profile(row.get("profile_url"))
        row["like_count"] = parse_human_count(row.get("like_count"))
        results.append(row)
    return results


# =========================================================
# 页面操作
# =========================================================
def try_click(page, texts, exact=False):
    clicked = 0
    for text in texts:
        try:
            locator = page.get_by_text(text, exact=exact)
            count = min(locator.count(), 10)
            for i in range(count):
                item = locator.nth(i)
                if item.is_visible(timeout=500):
                    item.click(timeout=1200)
                    clicked += 1
                    jitter(WAIT_AFTER_ACTION)
        except Exception:
            continue
    return clicked


def ensure_comment_panel(page):
    candidates = [
        "评论", "全部评论", "查看评论", "展开评论", "comment"
    ]
    for text in candidates:
        try:
            loc = page.get_by_text(text)
            if loc.count() > 0:
                for i in range(min(loc.count(), 5)):
                    item = loc.nth(i)
                    if item.is_visible(timeout=500):
                        item.click(timeout=1500)
                        jitter((1.0, 2.0))
                        return True
        except Exception:
            pass

    # 兜底：点击页面右侧常见评论按钮区域
    for pos in [{"x": 1320, "y": 460}, {"x": 1320, "y": 520}, {"x": 1280, "y": 500}]:
        try:
            page.mouse.click(pos["x"], pos["y"])
            jitter((1.0, 1.8))
            page.wait_for_timeout(500)
            if page.get_by_text("评论").count() or page.locator("a[href*='/profile/']").count():
                return True
        except Exception:
            pass
    return False


def expand_all_replies(page):
    texts = [
        "展开", "展开更多", "展开回复", "查看回复", "查看全部回复",
        "展开全部", "更多回复", "更多评论", "剩余", "楼中楼"
    ]
    total_clicked = 0
    stable_rounds = 0
    for _ in range(MAX_REPLY_EXPAND_ROUNDS):
        clicked = try_click(page, texts)
        total_clicked += clicked
        if clicked == 0:
            stable_rounds += 1
        else:
            stable_rounds = 0
        if stable_rounds >= 3:
            break
        try:
            page.mouse.wheel(0, 900)
            jitter(WAIT_AFTER_SCROLL)
        except Exception:
            pass
    return total_clicked


def scroll_comment_area(page):
    last_count = 0
    empty_rounds = 0
    for i in range(MAX_SCROLL_ROUNDS):
        try:
            count = page.locator("a[href*='/profile/']").count()
        except Exception:
            count = 0
        debug_print(f"[评论滚动] round={i + 1} visible_profiles={count}")

        if count <= last_count:
            empty_rounds += 1
        else:
            empty_rounds = 0
        last_count = count

        expand_all_replies(page)

        try:
            page.mouse.wheel(0, 1400)
            jitter(WAIT_AFTER_SCROLL)
        except Exception:
            pass

        if empty_rounds >= MAX_EMPTY_SCROLL_ROUNDS:
            break


def merge_comments(base_rows, new_rows):
    merged = {}
    for row in list(base_rows) + list(new_rows):
        key = (
            str(row.get("comment_id") or ""),
            str(row.get("parent_comment_id") or ""),
            str(row.get("content") or ""),
            str(row.get("user_id") or row.get("user_name") or "")
        )
        if key not in merged:
            merged[key] = dict(row)
        else:
            old = merged[key]
            for k, v in row.items():
                if v not in (None, "", [], {}):
                    if k == "like_count":
                        old[k] = max(parse_human_count(old.get(k)), parse_human_count(v))
                    else:
                        if not old.get(k):
                            old[k] = v
            if old.get("source") != "network" and row.get("source") == "network":
                old["source"] = "network"
            merged[key] = old
    return list(merged.values())


# =========================================================
# 统计与导出
# =========================================================
def compute_main_comment_scores(rows):
    by_comment_id = {}
    root_rows = []

    for row in rows:
        row["comment_id"] = str(row.get("comment_id") or "")
        row["parent_comment_id"] = str(row.get("parent_comment_id") or "")
        row["root_comment_id"] = str(row.get("root_comment_id") or row["comment_id"])
        row["like_count"] = parse_human_count(row.get("like_count"))
        by_comment_id[row["comment_id"]] = row

    for row in rows:
        if not row.get("parent_comment_id"):
            root_rows.append(row)

    root_ids = set(r["comment_id"] for r in root_rows if r.get("comment_id"))
    for row in rows:
        if row.get("parent_comment_id") and not row.get("root_comment_id"):
            cur = row
            seen = set()
            while cur.get("parent_comment_id") and cur.get("parent_comment_id") not in seen:
                seen.add(cur.get("parent_comment_id"))
                parent = by_comment_id.get(cur.get("parent_comment_id"))
                if not parent:
                    break
                cur = parent
            row["root_comment_id"] = cur.get("comment_id") or row.get("root_comment_id")
        if row.get("root_comment_id") in root_ids and row.get("comment_id") not in root_ids:
            row["level"] = 2

    children_map = {}
    for row in rows:
        rid = row.get("root_comment_id") or row.get("comment_id")
        children_map.setdefault(rid, []).append(row)

    main_rows = []
    for root in root_rows:
        rid = root.get("comment_id")
        branch = children_map.get(rid, [])
        sub_like_sum = sum(x.get("like_count", 0) for x in branch if x.get("comment_id") != rid)
        sub_count = sum(1 for x in branch if x.get("comment_id") != rid)
        total_like = root.get("like_count", 0) + sub_like_sum
        out = dict(root)
        out["self_like_count"] = root.get("like_count", 0)
        out["sub_comment_like_sum"] = sub_like_sum
        out["sub_comment_count"] = sub_count
        out["total_like_count"] = total_like
        main_rows.append(out)

    main_rows.sort(key=lambda x: (-x.get("total_like_count", 0), -x.get("self_like_count", 0), x.get("comment_id", "")))
    return main_rows, rows


def save_json(path, data):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_csv(path, rows, fields):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


# =========================================================
# 主流程
# =========================================================
def scrape_kuaishou_video_comments(video_url=VIDEO_URL):
    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STATE_DIR)
    ensure_dir(JSON_DIR)
    ensure_dir(CSV_DIR)

    if not video_url:
        raise ValueError("请先设置 VIDEO_URL")

    video_id = parse_video_id(video_url) or f"video_{abs(hash(video_url))}"
    cookies, headers = load_cookies_and_headers(COOKIES_FILE)
    all_comments = []

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
            nonlocal all_comments
            try:
                ctype = (resp.headers or {}).get("content-type", "")
                if "json" not in ctype and "javascript" not in ctype and "text/plain" not in ctype:
                    return
                url = (resp.url or "").lower()
                if not any(k in url for k in [
                    "comment", "reply", "subcomment", "graphql", "vision", "commentlist", "comment/list"
                ]):
                    return
                text = resp.text()
                if not text or len(text) < 10:
                    return
                try:
                    obj = json.loads(text)
                except Exception:
                    return
                comments = extract_comments_from_json(obj)
                if comments:
                    all_comments = merge_comments(all_comments, comments)
                    debug_print(f"[监听到评论接口] 新增/合并后评论数：{len(all_comments)} | {resp.url[:120]}")
            except Exception:
                pass

        page.on("response", on_response)

        print("=" * 60)
        print("打开目标视频页")
        print(video_url)
        print("=" * 60)
        page.goto(video_url, wait_until="domcontentloaded", timeout=HTTP_TIMEOUT)
        jitter(WAIT_AFTER_NAV)

        ensure_comment_panel(page)
        expand_all_replies(page)
        scroll_comment_area(page)
        expand_all_replies(page)

        dom_comments = extract_comments_from_dom(page)
        if dom_comments:
            all_comments = merge_comments(all_comments, dom_comments)
            debug_print(f"[DOM补充评论] 合并后评论数：{len(all_comments)}")

        browser.close()

    # 补 root/level
    for row in all_comments:
        row["video_id"] = video_id
        row["video_url"] = video_url
        row["user_id"] = str(row.get("user_id") or extract_user_id_from_profile(row.get("profile_url")))
        row["level"] = 1 if not row.get("parent_comment_id") else 2
        row["root_comment_id"] = str(row.get("root_comment_id") or row.get("comment_id"))

    main_comments_sorted, flat_rows = compute_main_comment_scores(all_comments)

    stem = safe_filename(video_id)
    all_json_path = os.path.join(JSON_DIR, f"{stem}_all_comments.json")
    main_json_path = os.path.join(JSON_DIR, f"{stem}_main_comments_sorted.json")
    flat_csv_path = os.path.join(CSV_DIR, f"{stem}_all_comments_flat.csv")
    main_csv_path = os.path.join(CSV_DIR, f"{stem}_main_comments_sorted.csv")

    save_json(all_json_path, flat_rows)
    save_json(main_json_path, main_comments_sorted)

    save_csv(
        flat_csv_path,
        flat_rows,
        fields=[
            "video_id", "video_url", "comment_id", "parent_comment_id", "root_comment_id", "level",
            "user_name", "user_id", "profile_url", "content", "like_count", "create_time", "source"
        ]
    )
    save_csv(
        main_csv_path,
        main_comments_sorted,
        fields=[
            "video_id", "video_url", "comment_id", "user_name", "user_id", "profile_url", "content",
            "self_like_count", "sub_comment_like_sum", "sub_comment_count", "total_like_count", "create_time", "source"
        ]
    )

    print("\n全部完成")
    print(f"总评论数（含楼中楼）：{len(flat_rows)}")
    print(f"主评论数：{len(main_comments_sorted)}")
    print(f"全部评论 JSON：{all_json_path}")
    print(f"主评论排序 JSON：{main_json_path}")
    print(f"全部评论 CSV：{flat_csv_path}"
          )
    print(f"主评论排序 CSV：{main_csv_path}")


if __name__ == "__main__":
    try:
        scrape_kuaishou_video_comments()
    except Exception as e:
        print(f"[主流程异常] {e}")
        print(traceback.format_exc())
