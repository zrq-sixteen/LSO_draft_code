ks_follow_feed_authors_latest5.py
爬取关注列表每个账号前五个视频并下载，提取账号基本信息和视频基本信息输出csv文件

ks_one_lastest5.py
└── 目标账号名称_目标账号ID/
    ├── account_profile.csv
    ├── latest_5_videos.csv
    ├── 01_xxxxx_视频标题.mp4
    ├── 02_xxxxx_视频标题.mp4
    ├── 03_xxxxx_视频标题.mp4
    ├── 04_xxxxx_视频标题.mp4
    └── 05_xxxxx_视频标题.mp4
运行
python ks_one_latest5.py --target "这里换成目标账号主页链接" --workers N
N为并发处理数


ks_recommend_feed.py
自动抓推荐页视频
记录作者信息到 reco_authors.csv
记录视频信息到 reco_videos.csv
下载每条推荐视频到 videos/ 目录
抓完当前批次后自动继续下一个推荐
自动重试
视频和作者去重
分批长休眠，降低风控概率
检测 captcha/verify 风控后自动停止
.part 临时文件下载，避免半截文件污染目录
输出目录
├── reco_authors.csv
├── reco_videos.csv
└── videos/
    ├── 0001_xxx_videoid.mp4
    ├── 0002_xxx_videoid.mp4
    └── ...

ks_video_comments.py

实现：
输入一个视频 URL，打开单个视频页
自动尝试打开评论区
自动滚动评论区，尽量加载全部主评论
自动点击“展开/更多回复/查看全部回复”等按钮，抓楼中楼
同时监听评论相关接口响应，优先从 JSON 抓结构化数据计算主评论总点赞量：
主评论总点赞 = 主评论自身点赞 + 该主评论下所有楼中楼点赞和将主评论按总点赞量降序输出

导出结果
json/<video_id>_all_comments.json
json/<video_id>_main_comments_sorted.json
csv/<video_id>_all_comments_flat.csv
csv/<video_id>_main_comments_sorted.csv

VIDEO_URL = "https://www.kuaishou.com/short-video/3xexample"