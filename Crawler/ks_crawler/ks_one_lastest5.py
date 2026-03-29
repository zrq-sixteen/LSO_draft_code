import os
import re
import csv
import time
import random
import argparse
import requests
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# 基础配置
# =========================================================
COOKIES_FILE = "./ks_cookies/1_zj.txt"
OUTPUT_ROOT = "/data3/jingzhang/program2/seed_data/"

MAX_VIDEOS = 5    #爬取最新5个视频
REQUEST_DELAY_RANGE = (1.5, 3.5)
DOWNLOAD_DELAY_RANGE = (0.8, 1.8)

MAX_DOWNLOAD_WORKERS = 4
HTTP_RETRIES = 3
HTTP_TIMEOUT = 20
DOWNLOAD_TIMEOUT = 60
CHUNK_SIZE = 1024 * 256

GRAPHQL_URL = "https://www.kuaishou.com/graphql"
DEBUG = True

illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n']


# =========================================================
# GraphQL 查询
# =========================================================
AUTHOR_QUERY = """
fragment photoContent on PhotoEntity {
  id
  duration
  caption
  originCaption
  likeCount
  viewCount
  realLikeCount
  coverUrl
  photoUrl
  photoH265Url
  timestamp
  coverUrls { url }
}

fragment feedContentWithLiveInfo on Feed {
  author {
    id
    name
    livingInfo
  }
  photo { ...photoContent }
  tags {
    type
    name
  }
}

query visionProfilePhotoList($pcursor: String, $userId: String, $page: String) {
  visionProfilePhotoList(pcursor: $pcursor, userId: $userId, page: $page) {
    feeds { ...feedContentWithLiveInfo }
    pcursor
  }
}
"""


# =========================================================
# 工具函数
# =========================================================
def jitter(delay_range):
    time.sleep(random.uniform(*delay_range))


def debug_print(*args):
    if DEBUG:
        print("[DEBUG]", *args)


def safe_filename(name, max_len=80):
    if not name:
        name = "unknown"
    for ch in illegal_chars:
        name = name.replace(ch, "")
    name = " ".join(name.strip().split())
    if not name:
        name = "unknown"
    return name[:max_len]


def ensure_dir(path):
    if path and not os.path.exists(path):
        os.makedirs(path)


def validate_video_file(filepath, min_size=1024):
    if not os.path.exists(filepath):
        return False
    if os.path.getsize(filepath) < min_size:
        return False

    try:
        with open(filepath, "rb") as f:
            head = f.read(512).lower()
        if b"<html" in head or b"<!doctype html" in head:
            return False
    except Exception:
        return False

    return True


# =========================================================
# cookies / session
# =========================================================
def load_cookies_and_headers(file_path):
    data = {}
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
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
    headers.setdefault("Content-Type", "application/json")
    headers.setdefault("Accept", "application/json, text/plain, */*")
    headers.setdefault("X-Requested-With", "XMLHttpRequest")
    headers["x-requested-with"] = "XMLHttpRequest"
    headers["accept"] = "*/*"

    return cookies, headers


def build_session(headers, cookies):
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

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(headers)
    session.cookies.update(cookies)
    return session


# =========================================================
# 解析账号ID
# =========================================================
def parse_user_id(target):
    target = str(target).strip()
    m = re.search(r"/profile/([^/?#]+)", target)
    return m.group(1) if m else target


# =========================================================
# 获取视频
# =========================================================
def build_query(user_id, pcursor=""):
    return {
        "operationName": "visionProfilePhotoList",
        "variables": {
            "userId": user_id,
            "pcursor": pcursor,
            "page": "profile"
        },
        "query": AUTHOR_QUERY.strip(),
    }


def send_graphql(session, data):
    jitter(REQUEST_DELAY_RANGE)
    try:
        resp = session.post(GRAPHQL_URL, json=data, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[GraphQL失败] {e}")
        try:
            print("响应片段：", resp.text[:500])
        except Exception:
            pass
        return None


def choose_video_url(photo):
    return photo.get("photoUrl") or photo.get("photoH265Url") or ""


def parse_videos(resp):
    block = resp.get("data", {}).get("visionProfilePhotoList", {}) or {}
    feeds = block.get("feeds", []) or []
    pcursor = block.get("pcursor", "")

    videos = []
    author_hint = {}

    for feed in feeds:
        photo = feed.get("photo", {}) or {}
        author = feed.get("author", {}) or {}

        if not photo:
            continue

        if author and not author_hint:
            author_hint = {
                "id": author.get("id", ""),
                "name": author.get("name", ""),
                "is_living": 1 if (author.get("livingInfo") or {}).get("living") else 0
            }

        video_url = choose_video_url(photo)
        if not video_url:
            continue

        timestamp = photo.get("timestamp", 0)
        publish_time = ""
        if timestamp:
            try:
                publish_time = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                publish_time = ""

        tags = []
        for tag in (feed.get("tags", []) or []):
            if isinstance(tag, dict) and tag.get("name"):
                tags.append(tag["name"])

        videos.append({
            "author_id": author.get("id", ""),
            "author_name": author.get("name", ""),
            "video_id": photo.get("id", ""),
            "video_title": photo.get("caption") or photo.get("originCaption") or "无标题",
            "video_url": video_url,
            "cover_url": photo.get("coverUrl") or ((photo.get("coverUrls") or [{}])[0].get("url", "")),
            "publish_time": publish_time,
            "duration_ms": photo.get("duration", 0),
            "likes": photo.get("realLikeCount", photo.get("likeCount", 0)),
            "views": photo.get("viewCount", 0),
            "video_tags": ",".join(tags) if tags else "",
        })

    return videos, pcursor, author_hint


def get_videos(session, user_id, n=5, max_pages=10):
    all_videos = []
    seen_video_ids = set()
    pcursor = ""
    hint = {}

    for _ in range(max_pages):
        data = build_query(user_id, pcursor)
        resp = send_graphql(session, data)
        if not resp:
            break

        videos, next_pcursor, cur_hint = parse_videos(resp)

        if cur_hint and not hint:
            hint = cur_hint

        for v in videos:
            vid = v.get("video_id")
            if vid and vid not in seen_video_ids:
                seen_video_ids.add(vid)
                all_videos.append(v)
                if len(all_videos) >= n:
                    break

        if len(all_videos) >= n:
            break

        if not next_pcursor or next_pcursor == pcursor:
            break

        pcursor = next_pcursor

    all_videos = sorted(all_videos, key=lambda x: x.get("publish_time", ""), reverse=True)
    return all_videos[:n], hint


# =========================================================
# 账号信息
# =========================================================
def get_profile(session, user_id, hint):
    url = f"https://www.kuaishou.com/profile/{user_id}"
    html = ""

    try:
        jitter(REQUEST_DELAY_RANGE)
        resp = session.get(url, timeout=HTTP_TIMEOUT, headers={"Referer": "https://www.kuaishou.com/"})
        resp.raise_for_status()
        html = resp.text
        url = resp.url
    except Exception as e:
        print(f"[主页请求失败] user_id={user_id}, err={e}")

    name = hint.get("name", "")
    intro = ""

    m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
    if m:
        intro = m.group(1).strip()

    if not name:
        m2 = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        if m2:
            name = m2.group(1).strip()

    return {
        "id": user_id,
        "name": name,
        "profile_url": url,
        "intro": intro,
        "is_living": hint.get("is_living", 0)
    }


# =========================================================
# 保存
# =========================================================
def save_profile(profile, out):
    filepath = os.path.join(out, "account_profile.csv")
    fields = ["id", "name", "profile_url", "intro", "is_living"]
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow(profile)
    print(f"已保存账号信息：{filepath}")


def save_videos(videos, out):
    filepath = os.path.join(out, "latest_5_videos.csv")
    fields = [
        "author_id",
        "author_name",
        "video_id",
        "video_title",
        "video_url",
        "cover_url",
        "publish_time",
        "duration_ms",
        "likes",
        "views",
        "video_tags"
    ]
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(videos)
    print(f"已保存视频信息：{filepath}")


# =========================================================
# 下载视频（并发 + 进度条）
# =========================================================
def download_video(session, video_info, output_dir, order_index):
    title = video_info.get("video_title", "无标题")
    safe_title = safe_filename(title, max_len=50)
    video_id = video_info.get("video_id", f"video_{order_index}")
    filename = f"{order_index:02d}_{video_id}_{safe_title}.mp4"
    filepath = os.path.join(output_dir, filename)

    if os.path.exists(filepath) and validate_video_file(filepath):
        return {
            "ok": True,
            "status": "skip",
            "filepath": filepath,
            "title": title
        }

    tmp_path = filepath + ".part"

    headers = {
        "Referer": "https://www.kuaishou.com/",
        "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0"),
        "Accept": "*/*",
    }

    try:
        jitter(DOWNLOAD_DELAY_RANGE)

        with session.get(
            video_info["video_url"],
            headers=headers,
            stream=True,
            timeout=DOWNLOAD_TIMEOUT,
            allow_redirects=True
        ) as resp:
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                raise ValueError(f"返回内容不是视频流，Content-Type={content_type}")

            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

        os.replace(tmp_path, filepath)

        if not validate_video_file(filepath):
            raise ValueError("下载结果文件异常，可能不是有效视频")

        return {
            "ok": True,
            "status": "downloaded",
            "filepath": filepath,
            "title": title
        }

    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

        return {
            "ok": False,
            "status": "failed",
            "filepath": filepath,
            "title": title,
            "error": str(e)
        }


def download_videos_parallel(session, videos, output_dir, max_workers=4):
    results = []
    if not videos:
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_video = {
            executor.submit(download_video, session, video, output_dir, idx): (idx, video)
            for idx, video in enumerate(videos, start=1)
        }

        with tqdm(total=len(future_to_video), desc="下载进度", unit="个") as pbar:
            for future in as_completed(future_to_video):
                result = future.result()
                results.append(result)

                if result["ok"]:
                    if result["status"] == "skip":
                        tqdm.write(f"跳过已存在：{result['filepath']}")
                    else:
                        tqdm.write(f"下载完成：{result['filepath']}")
                else:
                    tqdm.write(f"下载失败：{result['title']}，原因：{result.get('error', 'unknown')}")

                pbar.update(1)

    return results


# =========================================================
# 主函数
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="抓取单个快手账号信息、最新5个视频并并发下载")
    parser.add_argument("--target", required=True, help="目标账号主页链接或 user_id")
    parser.add_argument("--cookies-file", default=COOKIES_FILE, help="cookies/header 文件路径")
    parser.add_argument("--output-root", default=OUTPUT_ROOT, help="输出目录")
    parser.add_argument("--max-videos", type=int, default=MAX_VIDEOS, help="抓取视频数量")
    parser.add_argument("--workers", type=int, default=MAX_DOWNLOAD_WORKERS, help="下载并发数")
    args = parser.parse_args()

    user_id = parse_user_id(args.target)
    if not user_id:
        print("无法解析目标账号 user_id")
        return

    ensure_dir(args.output_root)

    cookies, headers = load_cookies_and_headers(args.cookies_file)
    session = build_session(headers, cookies)

    print("=" * 60)
    print(f"开始处理目标账号：{user_id}")
    print("=" * 60)

    print("\n[1/3] 抓取最新视频 ...")
    videos, hint = get_videos(session, user_id, n=args.max_videos, max_pages=10)
    print(f"获取到视频数量：{len(videos)}")

    print("\n[2/3] 抓取账号主页信息 ...")
    profile = get_profile(session, user_id, hint)

    folder = f"{safe_filename(profile['name'] or 'unknown', 50)}_{user_id}"
    out = os.path.join(args.output_root, folder)
    ensure_dir(out)

    save_profile(profile, out)

    if videos:
        for v in videos:
            if not v.get("author_name"):
                v["author_name"] = profile.get("name", "")
            if not v.get("author_id"):
                v["author_id"] = profile.get("id", user_id)

        save_videos(videos, out)
    else:
        print("未获取到视频数据")

    print("\n[3/3] 开始并发下载视频 ...")
    if videos:
        download_results = download_videos_parallel(
            session=session,
            videos=videos,
            output_dir=out,
            max_workers=args.workers
        )
        success_count = sum(1 for x in download_results if x.get("ok"))
        print(f"视频下载完成：{success_count}/{len(videos)}")
    else:
        print("没有可下载的视频")

    print("\n账号信息：")
    for k, v in profile.items():
        print(f"{k}: {v}")

    print(f"\n输出目录：{out}")
    print("完成。")


if __name__ == "__main__":
    main()
