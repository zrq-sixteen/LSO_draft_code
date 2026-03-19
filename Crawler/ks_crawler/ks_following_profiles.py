import os
import csv
import time
import random
import requests
from typing import Dict, List, Optional

# =========================================================
# 基础配置
# =========================================================
COOKIES_FILE = "./ks_cookies/1_zj.txt"
OUTPUT_DIR = "/data3/jingzhang/program2/seed_data/"
OUTPUT_CSV = "seed_profiles.csv"

REQUEST_DELAY_RANGE = (1.5, 3.0)
MAX_PAGES = 200

FOLLOWING_URL = "https://www.kuaishou.com/rest/v/relation/fol"

# 如果你后面抓到了“用户主页详情接口”，把它填到这里
# 例如：
# USER_DETAIL_URL = "https://www.kuaishou.com/rest/xxx/xxx"
USER_DETAIL_URL = None

ILLEGAL_CHARS = ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\n']


# =========================================================
# 通用工具
# =========================================================
def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def safe_text(value) -> str:
    if value is None:
        return ""
    s = str(value)
    for ch in ILLEGAL_CHARS:
        s = s.replace(ch, " ")
    return s.strip()


def sleep_a_bit() -> None:
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))


def load_cookies_and_headers(file_path: str):
    """
    cookie 文件格式示例：
    cookies = {...}
    headers = {...}
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    data = {}
    exec(content, {}, data)

    cookies = data.get("cookies", {})
    headers = data.get("headers", {})

    if not isinstance(cookies, dict):
        raise ValueError(f"cookies 无效：{file_path}")
    if not isinstance(headers, dict):
        raise ValueError(f"headers 无效：{file_path}")

    headers.setdefault("User-Agent", "Mozilla/5.0")
    headers.setdefault("Referer", "https://www.kuaishou.com/")
    headers.setdefault("Origin", "https://www.kuaishou.com")
    headers.setdefault("Content-Type", "application/json")
    headers.setdefault("Accept", "application/json, text/plain, */*")
    headers.setdefault("X-Requested-With", "XMLHttpRequest")

    return cookies, headers


# =========================================================
# 1) 关注列表接口
# =========================================================
def get_following_page(headers: Dict, cookies: Dict, pcursor: str = "") -> Optional[Dict]:
    payload = {
        "pcursor": pcursor,
        "ftype": 1
    }

    sleep_a_bit()

    try:
        resp = requests.post(
            FOLLOWING_URL,
            json=payload,
            headers=headers,
            cookies=cookies,
            timeout=20
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[ERROR] 关注列表请求失败: {e}")
        return None
    except ValueError as e:
        print(f"[ERROR] 关注列表返回 JSON 解析失败: {e}")
        return None


def normalize_following_user(raw_user: Dict) -> Dict:
    """
    根据你当前已抓到的 /rest/v/relation/fol 响应做标准化。
    """
    living_info = raw_user.get("livingInfo", {}) or {}

    return {
        "user_id": safe_text(raw_user.get("user_id", "")),
        "user_name": safe_text(raw_user.get("user_name", "")),
        "intro": safe_text(raw_user.get("user_text", "")),
        "gender": "",
        "age": "",
        "region": "",
        "has_shop": "",
        "is_living": 1 if living_info.get("living") else 0,
        "has_fans_group": "",
        "follower_count": "",
        "like_count": "",
        "profile_url": f'https://www.kuaishou.com/profile/{safe_text(raw_user.get("user_id", ""))}',
    }


def get_all_following_users(headers: Dict, cookies: Dict, max_pages: int = MAX_PAGES) -> List[Dict]:
    all_users: List[Dict] = []
    seen_user_ids = set()
    pcursor = ""

    for page in range(1, max_pages + 1):
        print(f"[INFO] 抓取关注列表，第 {page} 页，pcursor={pcursor!r}")

        resp_json = get_following_page(headers, cookies, pcursor=pcursor)
        if not resp_json:
            print("[WARN] 本页没有返回数据，停止。")
            break

        if resp_json.get("result") != 1:
            print(f"[WARN] 接口返回异常: {resp_json}")
            break

        fols = resp_json.get("fols", [])
        next_pcursor = resp_json.get("pcursor", "")

        if not fols:
            print("[INFO] 没有更多关注账号。")
            break

        new_count = 0
        for raw_user in fols:
            uid = safe_text(raw_user.get("user_id", ""))
            if not uid or uid in seen_user_ids:
                continue

            seen_user_ids.add(uid)
            all_users.append(normalize_following_user(raw_user))
            new_count += 1

        print(f"[INFO] 本页新增 {new_count} 个账号，累计 {len(all_users)} 个")

        if not next_pcursor or next_pcursor == pcursor:
            print("[INFO] 没有新的 pcursor，停止分页。")
            break

        pcursor = next_pcursor

    return all_users


# =========================================================
# 2) 用户详情接口（占位）
# =========================================================
def parse_user_detail_response(resp_json: Dict) -> Dict:
    """
    这里是一个通用模板。
    你抓到真实“主页详情接口”后，把字段路径替换掉即可。
    """
    data = resp_json.get("data", {}) if isinstance(resp_json, dict) else {}
    root = data or resp_json or {}

    user = (
        root.get("user") or
        root.get("profile") or
        root.get("userProfile") or
        root.get("visionProfile") or
        root.get("detail") or
        {}
    )

    def pick(*keys, default=""):
        for key in keys:
            value = user.get(key)
            if value not in (None, ""):
                return value
        return default

    return {
        "gender": pick("gender", "sex", default=""),
        "age": pick("age", default=""),
        "region": pick("cityName", "region", "location", "ipLocation", default=""),
        "has_shop": pick("hasShop", "shopOpen", "isShop", default=""),
        "is_living": pick("living", "isLiving", default=""),
        "has_fans_group": pick("hasFansGroup", "fansGroup", "hasGroup", default=""),
        "follower_count": pick("fan", "fansCount", "followerCount", "fans", default=""),
        "like_count": pick("liked", "likedCount", "likeCount", "totalLikeCount", default=""),
        "intro": pick("description", "brief", "bio", "user_text", default=""),
        "user_name": pick("name", "user_name", default=""),
    }


def get_user_detail(user_id: str, headers: Dict, cookies: Dict) -> Dict:
    """
    当前脚本先预留这个能力。
    只有你抓到真实详情接口后，再把 USER_DETAIL_URL 填上。
    """
    if not USER_DETAIL_URL:
        return {}

    payload = {"userId": user_id}
    sleep_a_bit()

    try:
        resp = requests.post(
            USER_DETAIL_URL,
            json=payload,
            headers=headers,
            cookies=cookies,
            timeout=20
        )
        resp.raise_for_status()
        resp_json = resp.json()
        return parse_user_detail_response(resp_json)
    except requests.RequestException as e:
        print(f"[WARN] 用户详情请求失败 {user_id}: {e}")
        return {}
    except ValueError as e:
        print(f"[WARN] 用户详情 JSON 解析失败 {user_id}: {e}")
        return {}


def enrich_users_with_detail(users: List[Dict], headers: Dict, cookies: Dict) -> List[Dict]:
    """
    如果配置了 USER_DETAIL_URL，则为每个用户补详情；
    没配置则原样返回。
    """
    if not USER_DETAIL_URL:
        print("[INFO] 未配置 USER_DETAIL_URL，跳过详情补全。")
        return users

    result = []
    total = len(users)

    for idx, user in enumerate(users, start=1):
        uid = user.get("user_id", "")
        print(f"[INFO] 补充详情 {idx}/{total}: {uid}")

        detail = get_user_detail(uid, headers, cookies)
        merged = user.copy()

        for k, v in detail.items():
            if v not in (None, ""):
                merged[k] = v

        result.append(merged)

    return result


# =========================================================
# 3) 导出 CSV
# =========================================================
def save_users_to_csv(users: List[Dict], output_dir: str, filename: str) -> str:
    ensure_dir(output_dir)
    filepath = os.path.join(output_dir, filename)

    fieldnames = [
        "user_id",
        "user_name",
        "intro",
        "gender",
        "age",
        "region",
        "has_shop",
        "is_living",
        "has_fans_group",
        "follower_count",
        "like_count",
        "profile_url",
    ]

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for user in users:
            row = {k: user.get(k, "") for k in fieldnames}
            writer.writerow(row)

    return filepath


# =========================================================
# 主流程
# =========================================================
def main():
    ensure_dir(OUTPUT_DIR)

    print("[INFO] 加载 cookies / headers")
    cookies, headers = load_cookies_and_headers(COOKIES_FILE)

    print("[INFO] 开始抓取关注列表")
    users = get_all_following_users(headers, cookies, max_pages=MAX_PAGES)
    print(f"[INFO] 共获取到 {len(users)} 个关注账号")

    print("[INFO] 开始补充账号详情")
    users = enrich_users_with_detail(users, headers, cookies)

    print("[INFO] 导出 CSV")
    csv_path = save_users_to_csv(users, OUTPUT_DIR, OUTPUT_CSV)
    print(f"[SUCCESS] 已导出: {csv_path}")


if __name__ == "__main__":
    main()
