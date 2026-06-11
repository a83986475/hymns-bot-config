# 🎵 Hymns Bot

赞美诗资源 Telegram Bot，支持 YouTube 下载、自动上传、SHA-256 去重。
代码推送到 main 分支后，自动构建 Docker 镜像并推送到 `ghcr.io`。

---

## 📁 目录结构

```
~/hymns-bot/               ← 部署根目录
├── docker-compose.yml    ← 从仓库拷贝到这里
├── .env                   （不提交）
└── bot/                   ← git clone 到这里
    ├── Dockerfile
    ├── bot.py
    ├── downloader.py
    ├── uploader.py
    ├── config.py
    ├── migrate_sha256.py
    ├── requirements.txt
    ├── yt-dlp.conf
    ├── cookies.txt            （不提交）
    └── .env.example
```

---

## 🚀 新服务器部署

```bash
# 1. 建立目录结构
mkdir -p ~/hymns-bot
git clone https://github.com/a83986475/hymns-bot-config ~/hymns-bot/bot

# 2. 拷贝 docker-compose.yml 到上层
cp ~/hymns-bot/bot/docker-compose.yml ~/hymns-bot/docker-compose.yml

# 3. 配置环境变量
cp ~/hymns-bot/bot/.env.example ~/hymns-bot/.env
nano ~/hymns-bot/.env

# 4. 配置 YouTube cookies
cp ~/hymns-bot/bot/cookies.example ~/hymns-bot/bot/cookies.txt
nano ~/hymns-bot/bot/cookies.txt

# 5. 登录 ghcr.io 并启动
echo <GITHUB_TOKEN> | docker login ghcr.io -u a83986475 --password-stdin
cd ~/hymns-bot && docker compose pull && docker compose up -d
```

---

## 🔄 更新代码

```bash
# 拉取最新代码和镜像
cd ~/hymns-bot/bot && git pull origin main
cp docker-compose.yml ~/hymns-bot/docker-compose.yml
cd ~/hymns-bot && docker compose pull && docker compose up -d
```

> 代码推送后 GitHub Actions 自动构建镜像，等待 3−5 分钟后再执行 `docker compose pull`。

---

## 🔧 日常操作

```bash
# 查看容器状态
cd ~/hymns-bot && docker compose ps

# 查看日志
docker compose logs -f
docker compose logs -f hymns-bot-0

# 重启
docker compose restart

# 停止 / 启动
docker compose down
docker compose up -d
```

---

## 🛠 SHA-256 历史数据回写

```bash
cd ~/hymns-bot/bot

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
