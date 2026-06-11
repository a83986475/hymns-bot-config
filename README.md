# 🎵 Hymns Bot

赞美诗资源 Telegram Bot，支持 YouTube 下载、自动上传、SHA-256 去重。

---

## 📁 目录结构

```
~/hymns-bot/               ← git clone 到这里
├── docker-compose.yml
├── .env               （不提交）
├── cookies.txt        （不提交）
├── bot.py
├── downloader.py
├── uploader.py
├── config.py
├── migrate_sha256.py
└── yt-dlp.conf
```

---

## 🚀 新服务器部署

```bash
# 1. 克隆代码
git clone https://github.com/a83986475/hymns-bot-config ~/hymns-bot

# 2. 配置环境变量
cp ~/hymns-bot/.env.example ~/hymns-bot/.env
nano ~/hymns-bot/.env

# 3. 配置 YouTube cookies
cp ~/hymns-bot/cookies.example ~/hymns-bot/cookies.txt
nano ~/hymns-bot/cookies.txt

# 4. 启动
cd ~/hymns-bot
docker compose up -d
```

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

# 更新代码后重建
cd ~/hymns-bot && git pull origin main
docker compose up -d --build
```

---

## 🔄 更新代码

```bash
cd ~/hymns-bot
git pull https://<GITHUB_TOKEN>@github.com/a83986475/hymns-bot-config.git main
docker compose restart
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
