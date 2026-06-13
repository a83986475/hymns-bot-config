# 🎵 Hymns Bot

赞美诗资源 Telegram Bot，支持 YouTube 下载、自动上传、SHA-256 去重。
代码推送到 main 分支后，GitHub Actions 自动构建 Docker 镜像推送到 `ghcr.io`。

---

## 📁 目录结构

```
~/hymns-bot/               ← 部署根目录（git clone 到这里）
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.base        ← 基础镜像（ffmpeg + Node.js，只构建一次）
├── bot.py
├── downloader.py
├── uploader.py
├── config.py
├── migrate_sha256.py
├── requirements.txt
├── yt-dlp.conf
├── cookies.example
├── cookies.txt            （不提交）
├── .env.example
└── .env                   （不提交）
```

---

## 🚀 新服务器部署

```bash
# 1. 克隆仓库
git clone https://github.com/a83986475/hymns-bot-config ~/hymns-bot
cd ~/hymns-bot

# 2. 配置环境变量
cp .env.example .env
nano .env

# 3. 配置 YouTube cookies
cp cookies.example cookies.txt
nano cookies.txt

# 4. 构建基础镜像（只需一次，ffmpeg + Node.js）
docker build -f Dockerfile.base -t hymns-bot-base:latest .

# 5. 构建业务镜像并启动
docker compose build
docker compose up -d

# 6. 验证
docker compose ps
```

---

## 🔄 更新代码

```bash
cd ~/hymns-bot
git pull
docker compose build        # 约 10 秒（依赖已缓存）
docker compose down && docker compose up -d
```

> 若 `requirements.txt` 有变动，使用 `docker compose build --no-cache` 强制重装依赖。

> `Dockerfile.base`（ffmpeg/Node.js）通常不需要重建。只有当这两个依赖版本需要升级时，才重新执行：
> ```bash
> docker build -f Dockerfile.base -t hymns-bot-base:latest .
> ```

---

## 🔧 日常操作

```bash
# 查看容器状态
cd ~/hymns-bot && docker compose ps

# 查看日志
docker compose logs -f
docker compose logs -f hymns-bot-0

# 重启（不重建镜像）
docker compose restart

# 停止 / 启动
docker compose down
docker compose up -d
```

---

## 🛠 SHA-256 历史数据回写

```bash
cd ~/hymns-bot

# 预览（不写入）
python3 migrate_sha256.py --dry-run

# 正式执行
python3 migrate_sha256.py

# 限制数量
python3 migrate_sha256.py --limit 100
```

---

## 🤖 Bot 指令

| 指令 | 说明 |
|------|------|
| `/search 关键词` | 搜索 YouTube 并列出候选 |
| `/auto 关键词` | 自动下载第一个（音频） |
| `/add URL` | 直接上传指定链接 |
| `/playlist URL` | 下载整个播放列表 |
| `/category 关键词 分类` | 指定分类上传 |

分类：`诗歌音频` `歌谱乐谱` `歌词文本` `教程资料` `油管上传`
