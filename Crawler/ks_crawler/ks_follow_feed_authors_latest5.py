import os
import csv
import time
import random
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

MAX_VIDEOS_PER_AUTHOR = 5
VIDEO_LEN_MS = None

REQUEST_DELAY_RANGE = (2, 5)
DOWNLOAD_DELAY_RANGE = (1, 3)

MAX_DOWNLOAD_WORKERS = 4
HTTP_RETRIES = 3
HTTP_TIMEOUT = 20
CHUNK_SIZE = 1024 * 128

GRAPHQL_URL = "https://www.kuaishou.com/graphql"
FOLLOWING_URL = "https://www.kuaishou.com/rest/v/relation/fol"

DEBUG = True

illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n']

# =========================================================
# GraphQL 查询：作者视频列表
# =========================================================
AUTHOR_QUERY = """
fragment photoContent on PhotoEntity {
  __typename
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
  manifest
  manifestH265
  videoResource
  coverUrls {
    url
    __typename
  }
  timestamp
  expTag
  animatedCoverUrl
  distance
  videoRatio
  liked
  stereoType
  profileUserTopPhoto
  musicBlocked
  riskTagContent
  riskTagUrl
}

fragment feedContentWithLiveInfo on Feed {
  type
  author {
    id
    name
    headerUrl
    following
    livingInfo
    headerUrls {
      url
      __typename
    }
    __typename
  }
  photo {
    ...photoContent
    __typename
  }
  canAddComment
  llsid
  status
  currentPcursor
  tags {
    type
    name
    __typename
  }
  __typename
}

query visionProfilePhotoList($pcursor: String, $userId: String, $page: String) {
  visionProfilePhotoList(pcursor: $pcursor, userId: $userId, page: $page) {
    result
    llsid
    webPageArea
    feeds {
      ...feedContentWithLiveInfo
      __typename
    }
    hostName
    pcursor
    __typename
  }
}
"""

# =========================================================
# Session / HTTP
# =========================================================
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
# 工具函数
# =========================================================
def jitter(delay_range):
    time.sleep(random.uniform(*delay_range))


def debug_print(msg):
    if DEBUG:
        print(msg)


def safe_filename(name, max_len=30):
    if not name:
        name = "无标题"

    for ch in illegal_chars:
        name = name.replace(ch, "")

    name = " ".join(name.strip().split())
    if not name:
        name = "无标题"

    return name[:max_len]


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


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
    headers.setdefault("Content-Type", "application/json")
    headers.setdefault("Accept", "application/json, text/plain, */*")
    headers.setdefault("X-Requested-With", "XMLHttpRequest")
    headers["x-requested-with"] = "XMLHttpRequest"
    headers["accept"] = "*/*"

    return cookies, headers


# =========================================================
# 关注列表（REST）
# =========================================================
def get_following_page(session, pcursor=""):
    payload = {
        "pcursor": pcursor,
        "ftype": 1
    }

    jitter(REQUEST_DELAY_RANGE)

    try:
        resp = session.post(FOLLOWING_URL, json=payload, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[关注接口失败] pcursor={pcursor!r}, err={e}")
        return None


def get_all_following_users(session, max_pages=100):
    all_users = []
    seen_user_ids = set()
    pcursor = ""

    for page in range(1, max_pages + 1):
        print(f"正在抓取关注列表，第 {page} 页，pcursor={pcursor!r}")

        resp = get_following_page(session, pcursor)
        if not resp:
            break

        if resp.get("result") != 1:
            print("关注接口返回异常：", resp)
            break

        fols = resp.get("fols", [])
        next_pcursor = resp.get("pcursor", "")

        if not fols:
            print("没有更多关注账号")
            break

        new_count = 0
        for u in fols:
            uid = u.get("user_id")
            if uid and uid not in seen_user_ids:
                seen_user_ids.add(uid)
                all_users.append({
                    "user_id": uid,
                    "user_name": u.get("user_name", "未知"),
                    "profile_url": f"https://www.kuaishou.com/profile/{uid}",
                    "intro": u.get("user_text", ""),
                    "is_living": 1 if (u.get("livingInfo") or {}).get("living") else 0,
                })
                new_count += 1

        print(f"本页新增 {new_count} 个账号，累计 {len(all_users)} 个")

        if not next_pcursor or next_pcursor == pcursor:
            print("没有新的 pcursor，停止分页。")
            break

        pcursor = next_pcursor

    return all_users


def save_following_csv(following_users, output_dir):
    filepath = os.path.join(output_dir, "following_accounts.csv")
    fields = ["user_id", "user_name", "profile_url", "intro", "is_living"]
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(following_users)
    print(f"已保存关注列表：{filepath}")


# =========================================================
# 作者视频（GraphQL）
# =========================================================
def build_author_json_data(user_id, pcursor=""):
    return {
        "operationName": "visionProfilePhotoList",
        "variables": {
            "userId": user_id,
            "pcursor": pcursor,
            "page": "profile"
        },
        "query": AUTHOR_QUERY.strip()
    }


def send_graphql(session, json_data):
    jitter(REQUEST_DELAY_RANGE)
    try:
        resp = session.post(GRAPHQL_URL, json=json_data, timeout=HTTP_TIMEOUT)
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
    """
    优先选择最直接可下载的地址。
    """
    return (
        photo.get("photoUrl")
        or photo.get("photoH265Url")
        or ""
    )


def parse_author_videos(resp_json, limit=5):
    data = resp_json.get("data", {})
    block = data.get("visionProfilePhotoList", {})
    feeds = block.get("feeds", [])
    next_pcursor = block.get("pcursor", "")

    debug_print(f"[DEBUG] 当前页 feeds 数量: {len(feeds)}, next_pcursor={next_pcursor!r}")

    videos = []
    for i, feed in enumerate(feeds, start=1):
        photo = feed.get("photo", {})
        if not photo:
            debug_print(f"[DEBUG] 第{i}条: 没有 photo")
            continue

        duration = photo.get("duration", 0)
        photo_url = photo.get("photoUrl")
        photo_h265_url = photo.get("photoH265Url")
        manifest = photo.get("manifest")
        caption = photo.get("caption", "")

        debug_print(
            f"[DEBUG] 第{i}条: title={caption[:20]!r}, duration={duration}, "
            f"photoUrl={'Y' if photo_url else 'N'}, "
            f"photoH265Url={'Y' if photo_h265_url else 'N'}, "
            f"manifest={'Y' if manifest else 'N'}"
        )

        if VIDEO_LEN_MS is not None and duration > VIDEO_LEN_MS:
            debug_print(f"[DEBUG] 第{i}条: 被时长过滤")
            continue

        video_url = choose_video_url(photo)
        if not video_url:
            debug_print(f"[DEBUG] 第{i}条: 没有可用 video_url")
            continue

        timestamp = photo.get("timestamp", 0)
        publish_time = ""
        if timestamp:
            publish_time = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")

        tags = []
        for tag in (feed.get("tags", []) or []):
            if isinstance(tag, dict) and "name" in tag:
                tags.append(tag["name"])

        videos.append({
            "video_id": photo.get("id", ""),
            "video_title": caption or "无标题",
            "video_url": video_url,
            "duration": duration,
            "publish_time": publish_time,
            "likes": photo.get("realLikeCount", 0),
            "views": photo.get("viewCount", 0),
            "author_id": feed.get("author", {}).get("id", ""),
            "author_name": feed.get("author", {}).get("name", "未知"),
            "video_tags": ",".join(tags) if tags else "",
        })

        if len(videos) >= limit:
            break

    return videos, next_pcursor


def get_latest_n_videos_of_author(session, user_id, n=5, max_pages=10):
    """
    修复点：
    - 某一页没解析出视频，不直接 break
    - 只要 next_pcursor 还在，就继续翻页
    - 去重
    """
    all_videos = []
    seen_video_ids = set()
    pcursor = ""

    for page_no in range(1, max_pages + 1):
        json_data = build_author_json_data(user_id, pcursor=pcursor)
        resp_json = send_graphql(session, json_data)
        if not resp_json:
            debug_print(f"[DEBUG] user={user_id} 第{page_no}页 GraphQL 无响应")
            break

        videos, next_pcursor = parse_author_videos(resp_json, limit=n)

        debug_print(
            f"[DEBUG] user={user_id} 第{page_no}页解析到 {len(videos)} 条视频, "
            f"next_pcursor={next_pcursor!r}"
        )

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
    return all_videos[:n]


def save_videos_csv(all_video_rows, output_dir):
    filepath = os.path.join(output_dir, "latest_5_videos.csv")
    fields = [
        "author_id", "author_name",
        "video_id", "video_title", "video_url",
        "publish_time", "duration", "likes", "views", "video_tags"
    ]
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_video_rows)
    print(f"已保存视频信息：{filepath}")


# =========================================================
# 并发下载
# =========================================================
def download_video(session, video_info, author_output_dir, order_index):
    safe_title = safe_filename(video_info.get("video_title", "无标题"))
    filename = f"{order_index:02d}_{safe_title}.mp4"
    filepath = os.path.join(author_output_dir, filename)

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        return {
            "ok": True,
            "status": "skip",
            "filepath": filepath,
            "title": video_info.get("video_title", "")
        }

    tmp_path = filepath + ".part"

    try:
        jitter(DOWNLOAD_DELAY_RANGE)

        with session.get(
            video_info["video_url"],
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

        return {
            "ok": True,
            "status": "downloaded",
            "filepath": filepath,
            "title": video_info.get("video_title", "")
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
            "title": video_info.get("video_title", ""),
            "error": str(e)
        }


def download_videos_parallel(session, videos, author_output_dir, max_workers=4):
    results = []

    if not videos:
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_video = {
            executor.submit(download_video, session, video, author_output_dir, idx): (idx, video)
            for idx, video in enumerate(videos, start=1)
        }

        for future in as_completed(future_to_video):
            result = future.result()
            results.append(result)

            if result["ok"]:
                if result["status"] == "skip":
                    print(f"跳过已存在：{result['filepath']}")
                else:
                    print(f"下载完成：{result['filepath']}")
            else:
                print(f"下载失败：{result['title']}，原因：{result.get('error', 'unknown')}")

    return results


# =========================================================
# 主流程
# =========================================================
def main():
    ensure_dir(OUTPUT_ROOT)

    cookies, headers = load_cookies_and_headers(COOKIES_FILE)
    session = build_session(headers, cookies)

    print("=" * 60)
    print("第 1 步：抓取关注列表")
    print("=" * 60)

    following_users = get_all_following_users(session)
    if not following_users:
        print("没有获取到关注列表")
        return

    save_following_csv(following_users, OUTPUT_ROOT)

    print("=" * 60)
    print("第 2 步：抓取每个关注账号的最新 5 个视频并并发下载")
    print("=" * 60)

    all_video_rows = []

    for idx, user in enumerate(tqdm(following_users, desc="处理关注账号"), start=1):
        user_id = user.get("user_id", "")
        user_name = safe_filename(user.get("user_name", f"user_{idx}"))
        if not user_id:
            print(f"跳过无 user_id 的账号：{user}")
            continue

        print(f"\n[{idx}/{len(following_users)}] 正在处理：{user_name} ({user_id})")
        author_output_dir = os.path.join(OUTPUT_ROOT, f"{user_name}_{user_id}")
        ensure_dir(author_output_dir)

        videos = get_latest_n_videos_of_author(session, user_id, n=MAX_VIDEOS_PER_AUTHOR, max_pages=10)
        if not videos:
            print(f"该账号没有获取到视频：{user_name}")
            continue

        all_video_rows.extend(videos)

        download_videos_parallel(
            session=session,
            videos=videos,
            author_output_dir=author_output_dir,
            max_workers=MAX_DOWNLOAD_WORKERS
        )

    save_videos_csv(all_video_rows, OUTPUT_ROOT)
    print("\n全部完成！")


if __name__ == "__main__":
    main()
