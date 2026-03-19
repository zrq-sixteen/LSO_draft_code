from flask import Flask, render_template_string, send_from_directory, abort, request, url_for
from pathlib import Path

app = Flask(__name__)

VIDEO_ROOT = Path("/data3/jingzhang/program2/seed_data")

HTML = """
<!doctype html>
<html lang="zh">
<head>
    <meta charset="utf-8">
    <title>视频示例浏览</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
            background: #f7f7f7;
        }
        h1 {
            margin-bottom: 12px;
        }
        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 12px;
        }
        .folder {
            background: white;
            border-radius: 12px;
            padding: 16px 20px;
            margin-bottom: 24px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        .folder h2 {
            margin-top: 0;
            font-size: 22px;
            color: #333;
        }
        .video-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
            gap: 16px;
        }
        .video-card {
            background: #fafafa;
            border: 1px solid #ddd;
            border-radius: 10px;
            padding: 12px;
        }
        .video-card p {
            margin: 0 0 8px 0;
            font-size: 14px;
            word-break: break-all;
        }
        video {
            width: 100%;
            max-height: 260px;
            border-radius: 8px;
            background: black;
        }
        .nav-buttons {
            display: flex;
            gap: 12px;
            align-items: center;
        }
        .btn {
            display: inline-block;
            padding: 10px 16px;
            background: #1677ff;
            color: white;
            text-decoration: none;
            border-radius: 8px;
            font-size: 14px;
        }
        .btn:hover {
            background: #0958d9;
        }
        .btn.disabled {
            background: #bfbfbf;
            pointer-events: none;
            cursor: not-allowed;
        }
        .info {
            color: #555;
            font-size: 15px;
        }
    </style>
</head>
<body>
    <h1>视频示例浏览</h1>

    <div class="topbar">
        <div class="info">
            当前文件夹：<strong>{{ current_folder }}</strong>
            （第 {{ current_idx + 1 }} / {{ total }} 个）
        </div>

        <div class="nav-buttons">
            {% if current_idx > 0 %}
                <a class="btn" href="{{ url_for('index', idx=current_idx - 1) }}">上一个</a>
            {% else %}
                <span class="btn disabled">上一个</span>
            {% endif %}

            {% if current_idx < total - 1 %}
                <a class="btn" href="{{ url_for('index', idx=current_idx + 1) }}">下一个</a>
            {% else %}
                <span class="btn disabled">下一个</span>
            {% endif %}
        </div>
    </div>

    <div class="folder">
        <h2>{{ current_folder }}</h2>
        <div class="video-grid">
            {% for f in files %}
            <div class="video-card">
                <p>{{ f }}</p>
                <video controls preload="metadata">
                    <source src="{{ url_for('serve_video', folder=current_folder, filename=f) }}" type="video/mp4">
                    你的浏览器不支持 video 标签。
                </video>
            </div>
            {% endfor %}
        </div>
    </div>
</body>
</html>
"""

@app.route("/")
def index():
    if not VIDEO_ROOT.exists():
        return f"目录不存在: {VIDEO_ROOT}", 404

    folders = []
    for subdir in sorted(VIDEO_ROOT.iterdir()):
        if subdir.is_dir():
            mp4_files = sorted([
                p.name for p in subdir.iterdir()
                if p.is_file() and p.suffix.lower() == ".mp4"
            ])
            if mp4_files:
                folders.append({
                    "name": subdir.name,
                    "files": mp4_files
                })

    if not folders:
        return "未找到任何 mp4 视频。", 404

    try:
        idx = int(request.args.get("idx", 0))
    except ValueError:
        idx = 0

    if idx < 0:
        idx = 0
    if idx >= len(folders):
        idx = len(folders) - 1

    current = folders[idx]

    return render_template_string(
        HTML,
        current_folder=current["name"],
        files=current["files"],
        current_idx=idx,
        total=len(folders)
    )

@app.route("/video/<path:folder>/<path:filename>")
def serve_video(folder, filename):
    folder_path = VIDEO_ROOT / folder
    file_path = folder_path / filename

    if not folder_path.exists() or not file_path.exists() or file_path.suffix.lower() != ".mp4":
        abort(404)

    return send_from_directory(folder_path, filename)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8849, debug=False)
