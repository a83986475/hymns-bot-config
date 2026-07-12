import asyncio
import logging
import os
import random
import re
import shutil
import time
import aiohttp
from aiohttp import web as aiohttp_web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)
from telegram.helpers import escape_markdown as _esc_md
from config import config
from downloader import search_youtube, get_formats, get_playlist_info, download_audio, download_video, SUPPORTED_HEIGHTS, HEIGHT_LABELS, AUDIO_QUALITY_PRESETS, COOKIE_FILE, _pot_provider_alive, POT_PROVIDER_URL
from uploader import direct_upload, refresh_jwt

# Telegram Bot Application 全局引用（用于向管理员发消息）
_bot_app: Application = None

# 每 bot 最多 1 个并发下载+上传任务，防止 Telegram flood control
_task_semaphore = asyncio.Semaphore(1)

# 全局 YouTube 请求速率限制：每次请求后至少等待 3 秒
_last_youtube_request = 0.0
_youtube_request_lock = asyncio.Lock()


async def _rate_limit_youtube():
    """限制 YouTube API 请求频率，每次请求后至少间隔 3 秒。"""
    global _last_youtube_request
    async with _youtube_request_lock:
        now = time.monotonic()
        elapsed = now - _last_youtube_request
        min_gap = random.uniform(2.0, 4.0)
        if elapsed < min_gap:
            await asyncio.sleep(min_gap - elapsed)
        _last_youtube_request = time.monotonic()

# ── YouTube 限流冷却缓存 ──
_yt_rate_limit_cache: dict[str, float] = {}  # video_id → 首次限流时间戳 (monotonic)
_YT_RATE_LIMIT_COOLDOWN = 3600  # 冷却 1 小时


def _check_yt_rate_limit(url: str) -> str:
    """检查 URL 是否在限流冷却期内。返回 '' 或错误消息。"""
    video_id = _get_video_id(url)
    if not video_id:
        return ''
    ts = _yt_rate_limit_cache.get(video_id)
    if ts is not None:
        remaining = time.monotonic() - ts
        if remaining < _YT_RATE_LIMIT_COOLDOWN:
            left_min = int((_YT_RATE_LIMIT_COOLDOWN - remaining) // 60)
            return f'视频 {video_id} 仍在 YouTube 限流冷却中（剩余约 {left_min} 分钟）'
    return ''


def _mark_yt_rate_limit(url: str):
    """标记视频被 YouTube 限流。video_id → 当前时间戳。"""
    video_id = _get_video_id(url)
    if video_id:
        logger.warning(f'🚫 标记限流: {video_id}，冷却 {_YT_RATE_LIMIT_COOLDOWN // 60} 分钟')
        _yt_rate_limit_cache[video_id] = time.monotonic()
        # 限制缓存大小，清理过期条目
        if len(_yt_rate_limit_cache) > 1000:
            now = time.monotonic()
            expired = [k for k, v in _yt_rate_limit_cache.items() if now - v > _YT_RATE_LIMIT_COOLDOWN * 2]
            for k in expired:
                del _yt_rate_limit_cache[k]

# 用户取消频道/播放列表下载的请求（set of user_id）
_channel_cancel_reqs: set[int] = set()

# 频道目录 ID 缓存（避免重复 API 调用）
_channel_folder_id_cache: dict[str, int] = {}  # channel_title -> folder_id

# ── 下载续传 checkpoint ──
# 记录已成功处理的视频 ID，Bot 重启或 SSH 断连后重新下载时可跳过已完成项
_CHANNEL_CHECKPOINT_DIR = os.environ.get('CHANNEL_CHECKPOINT_DIR', config.DOWNLOAD_DIR)


def _get_video_id_from_url(url: str) -> str:
    """从 YouTube URL 提取视频 ID"""
    m = re.search(r'(?:v=|youtu\.be/|shorts/)([a-zA-Z0-9_-]{11})', url)
    return m.group(1) if m else ''


def _load_checkpoint(channel_key: str) -> set:
    """加载续传 checkpoint，返回已处理过的视频 ID 集合。"""
    path = os.path.join(_CHANNEL_CHECKPOINT_DIR, f'checkpoint_{channel_key}.json')
    try:
        with open(path, 'r') as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _save_checkpoint(channel_key: str, video_id: str):
    """记录一个视频 ID 到 checkpoint（已成功处理）。"""
    path = os.path.join(_CHANNEL_CHECKPOINT_DIR, f'checkpoint_{channel_key}.json')
    try:
        processed = _load_checkpoint(channel_key)
        processed.add(video_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(list(processed), f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f'checkpoint 写入失败: {e}')


def _clear_checkpoint(channel_key: str):
    """全部完成后清理 checkpoint。"""
    path = os.path.join(_CHANNEL_CHECKPOINT_DIR, f'checkpoint_{channel_key}.json')
    try:
        if os.path.exists(path):
            os.remove(path)
            logger.info(f'checkpoint 已清理: {channel_key}')
    except Exception:
        pass

# 追踪当前正在通过 HTTP 流式传输的文件（磁盘满时紧急清理用）
_active_streams: set = set()

# 磁盘空间警戒线：剩余空间低于此值时触发预防性清理（保护 VPS 上其他服务）
_MIN_FREE_SPACE = 2 * 1024**3  # 2GB

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

search_cache:   dict = {}   # uid -> [results]
format_cache:   dict = {}   # uid -> {url, formats_info}
playlist_cache: dict = {}   # uid -> {entries, title, count}

# 每个缓存的条目上限，防止长期运行后内存泄漏
_MAX_CACHE_SIZE = 100

def _cache_put(cache: dict, key, value, max_size: int = _MAX_CACHE_SIZE):
    """插入缓存项，超出上限时淘汰最旧的条目（Python 3.7+ dict 保持插入顺序）。"""
    cache[key] = value
    while len(cache) > max_size:
        oldest = next(iter(cache))
        del cache[oldest]


def is_admin(user_id: int) -> bool:
    return not config.ADMIN_IDS or user_id in config.ADMIN_IDS

def fmt_dur(seconds) -> str:
    s = int(seconds or 0)
    return f"{s//60}:{s%60:02d}"

def _height_label(h: int) -> str:
    return HEIGHT_LABELS.get(h, f'{h}p')

# ──────────────────命令──────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    aq = AUDIO_QUALITY_PRESETS
    await update.effective_message.reply_text(
        "🎵 *赞美诗资源机器人*\n\n"
        "*/search* `关键词` — 搜索并列出候选\n"
        "*/auto* `关键词` — 自动下载第一个（音频）\n"
        "*/add* `URL` — 直接上传指定链接（可选音质）\n"
        "*/playlist* `URL` — 下载整个播放列表\n"
        "*/channel* `URL` — 下载整个频道（音频）\n"
        "*/category* `关键词` `分类` — 指定分类上传\n"
        "*/cancel* — 取消正在进行的频道/播放列表下载\n\n"
        f"*音质选项*：\n"
        f"`{aq['low'][0]}` — {aq['low'][3]}（最省流量）\n"
        f"`{aq['medium'][0]}` — {aq['medium'][3]}\n"
        f"`{aq['high'][0]}` — {aq['high'][3]}\n"
        "`192k` — 原质 stereo（默认，不压缩）\n"
        "省略音质时使用原质 192k\n\n"
        "分类：`诗歌音频` `歌谱乐谱` `歌词文本` `教程资料` `油管上传`",
        parse_mode='Markdown'
    )

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not ctx.args:
        await update.effective_message.reply_text('用法：/search 奇异恩典'); return
    keyword = ' '.join(ctx.args)
    msg = await update.effective_message.reply_text(f'🔍 搜索中：{keyword}...')
    try:
        results = search_youtube(keyword, max_results=5)
        if not results:
            await msg.edit_text('❌ 未找到结果'); return
        _cache_put(search_cache, update.effective_user.id, results)
        lines, buttons = [], []
        for r in results:
            dur = fmt_dur(r.get('duration'))
            title = _esc_md(str(r['title']))
            uploader = _esc_md(str(r.get('uploader', '')))
            lines.append(f"`{r['index']}`. {title} [{dur}]\n   _{uploader}_")
            buttons.append([InlineKeyboardButton(
                f"⬇️ {r['index']}. {r['title'][:35]}",
                callback_data=f"pick:{update.effective_user.id}:{r['index']-1}"
            )])
        await msg.edit_text(
            f"🎵 *{_esc_md(keyword)}* 结果：\n\n" + '\n\n'.join(lines),
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode='Markdown'
        )
    except Exception as e:
        await msg.edit_text(f'❌ 搜索失败：{e}')

async def cmd_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not ctx.args:
        await update.effective_message.reply_text('用法：/auto 奇异恩典'); return
    keyword = ' '.join(ctx.args)
    msg = await update.effective_message.reply_text(f'⚡ 自动处理：{keyword}...')
    try:
        results = search_youtube(keyword, max_results=1)
        if not results:
            await msg.edit_text('❌ 未找到资源'); return
        await _do_download_and_upload(msg, results[0]['url'], {}, 'audio', None)
    except Exception as e:
        await msg.edit_text(f'❌ 失败：{e}')

async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not ctx.args:
        await update.effective_message.reply_text('用法：/add https://youtube.com/...'); return
    await _show_format_picker(update.effective_message, ctx.args[0], update.effective_user.id)

async def cmd_category(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if len(ctx.args) < 2:
        await update.effective_message.reply_text('用法：/category 奇异恩典 诗歌音频'); return
    category = ctx.args[-1]
    keyword = ' '.join(ctx.args[:-1])
    msg = await update.effective_message.reply_text(f'⚡ 搜索「{category}」：{keyword}...')
    try:
        results = search_youtube(keyword, max_results=1)
        if not results:
            await msg.edit_text('❌ 未找到资源'); return
        await _do_download_and_upload(msg, results[0]['url'], {'category': category}, 'audio', None)
    except Exception as e:
        await msg.edit_text(f'❌ 失败：{e}')


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """取消正在进行的频道/播放列表下载。"""
    if not is_admin(update.effective_user.id): return
    uid = update.effective_user.id
    already = uid in _channel_cancel_reqs
    _channel_cancel_reqs.add(uid)  # 先标记再回复（即使回复失败，取消效果仍在）
    logger.info(f'用户 {uid} 发送取消请求（already={already}）')
    try:
        if already:
            await update.effective_message.reply_text('⏳ 已有取消请求在处理中...')
        else:
            await update.effective_message.reply_text(
                '🛑 取消请求已发送，将在当前项处理完成后停止...\n'
                '（如果当前正在上传中，请稍等片刻）'
            )
    except Exception as e:
        logger.warning(f'取消回复失败（可能是频率限制）: {e}')
        # 即使回复失败，取消标志已设置，不影响实际取消

async def cmd_playlist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not ctx.args:
        await update.effective_message.reply_text('用法：/playlist https://youtube.com/playlist?list=...'); return
    url = ctx.args[0]
    uid = update.effective_user.id
    msg = await update.effective_message.reply_text('🔍 正在解析播放列表...')
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, get_playlist_info, url)
    except Exception as e:
        await msg.edit_text(f'❌ 解析失败：{e}'); return

    if not info['entries']:
        await msg.edit_text('❌ 播放列表为空'); return

    _cache_put(playlist_cache, uid, info)
    total_dur = fmt_dur(info['total_duration'])

    aq = AUDIO_QUALITY_PRESETS
    buttons = [
        [InlineKeyboardButton(f'🎵 全部音频 ({aq["low"][3]})', callback_data=f'pl:{uid}:audio:low')],
        [InlineKeyboardButton(f'🎵 全部音频 ({aq["medium"][3]})', callback_data=f'pl:{uid}:audio:medium')],
        [InlineKeyboardButton(f'🎵 全部音频 ({aq["high"][3]})', callback_data=f'pl:{uid}:audio:high')],
        [InlineKeyboardButton('🎵 全部音频 (原质 192k)', callback_data=f'pl:{uid}:audio:0')],
        [InlineKeyboardButton('🎬 最高画质视频', callback_data=f'pl:{uid}:video:best')],
    ]
    for h in sorted(SUPPORTED_HEIGHTS):
        buttons.append([InlineKeyboardButton(
            f'🎬 全部视频 {_height_label(h)}',
            callback_data=f'pl:{uid}:video:{h}'
        )])

    await msg.edit_text(
        f"📋 *{_esc_md(str(info['title']))}*\n"
        f"🎵 共 {info['count']} 个视频\n"
        f"⏱ 总时长：{total_dur}\n\n"
        f"请选择下载格式：",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='Markdown'
    )


async def _discover_channel_playlists(url: str, loop) -> list:
    """发现频道中的播放列表：访问 /playlists 标签页提取。"""
    # 构造播放列表标签页 URL（去掉末尾 /videos 等）
    import json as _json
    import subprocess as _sp
    base = re.sub(r'(/videos|/playlists|/streams)?/?$', '', url)
    playlists_url = base + '/playlists'
    logger.info(f'发现频道播放列表: {playlists_url}')

    def _fetch():
        cmd = ['yt-dlp', '--no-warnings', '--flat-playlist', '--dump-single-json', '--ignore-errors']
        if os.path.exists(COOKIE_FILE):
            cmd += ['--cookies', COOKIE_FILE]
        # 合并 extractor-args（多个参数用逗号分隔合并为一条，否则后面的会覆盖前面的）
        extractor_parts = ['youtubetab:skip=authcheck']
        if _pot_provider_alive():
            extractor_parts.append(f'youtube:youtubepot-bgutilhttp=base_url={POT_PROVIDER_URL}')
        cmd += ['--extractor-args', ','.join(extractor_parts)]
        cmd.append(playlists_url)
        r = _sp.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
        if r.returncode != 0:
            stderr = r.stderr.strip()
            logger.warning(f'yt-dlp 获取频道播放列表失败 (returncode={r.returncode}): {stderr[:500] if stderr else "无错误输出"}')
            return []
        if not r.stdout.strip():
            logger.warning(f'yt-dlp 获取频道播放列表返回空 stdout: {playlists_url}')
            return []
        try:
            obj = _json.loads(r.stdout)
        except Exception as e:
            logger.warning(f'解析频道播放列表 JSON 失败: {e}')
            return []
        if obj is None:
            logger.warning(f'yt-dlp 返回 null (可能无权限访问此页面): {playlists_url}')
            return []
        entries = obj.get('entries', [])
        result = []
        for e in entries:
            title = e.get('title', '')
            pid = e.get('id', '')
            if title and pid:
                result.append({
                    'title': title,
                    'id': pid,
                    'url': f'https://www.youtube.com/playlist?list={pid}',
                })
        return result

    return await loop.run_in_executor(None, _fetch)


async def cmd_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """下载整个 YouTube 频道并自动上传到 Telegram。
    发现播放列表并按 频道名/播放列表名 组织目录结构。
    """
    if not is_admin(update.effective_user.id): return
    if not ctx.args:
        await update.effective_message.reply_text(
            '用法：/channel https://youtube.com/@channelname\n'
            '例：/channel https://www.youtube.com/@LigonierMinistries'
        )
        return
    url = ctx.args[0]
    # @ 简写补全
    url = url.strip()
    if url.startswith('@'):
        url = 'https://www.youtube.com/' + url
    elif not url.startswith('http'):
        # 裸频道名也补全（如 TheHopeTV → https://www.youtube.com/@TheHopeTV）
        url = 'https://www.youtube.com/@' + url
    uid = update.effective_user.id
    msg = await update.effective_message.reply_text('🔍 正在解析频道...')
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, get_playlist_info, url)
    except Exception as e:
        await msg.edit_text(f'❌ 解析频道失败：{e}'); return

    if not info['entries']:
        await msg.edit_text('❌ 频道为空或无法获取视频列表'); return

    # 尝试发现播放列表
    playlists = await _discover_channel_playlists(url, loop)
    channel_title = info['title']

    _cache_put(playlist_cache, uid, {'info': info, 'playlists': playlists, 'channel_title': channel_title})
    total_dur = fmt_dur(info['total_duration'])
    total = info['count']

    # 构建 UI
    playlist_count = len(playlists)
    lines = [f"📺 *{_esc_md(str(channel_title))}*"]
    lines.append(f"🎵 共 {total} 个视频")
    lines.append(f"⏱ 总时长：{total_dur}")
    if playlist_count > 0:
        lines.append(f"📂 发现 {playlist_count} 个播放列表")
        # 显示前 10 个播放列表
        for pl in playlists[:10]:
            lines.append(f"   • {_esc_md(str(pl['title']))}")
        if len(playlists) > 10:
            lines.append(f'   … 还有 {len(playlists) - 10} 个')
    lines.append(f"\n文件将按 频道名/播放列表名 组织。")

    aq = AUDIO_QUALITY_PRESETS
    buttons = [
        [InlineKeyboardButton(f'✅ 全部音频 ({aq["low"][3]}) ({total} 个)', callback_data=f'ch:{uid}:audio:low')],
        [InlineKeyboardButton(f'✅ 全部音频 ({aq["medium"][3]}) ({total} 个)', callback_data=f'ch:{uid}:audio:medium')],
        [InlineKeyboardButton(f'✅ 全部音频 ({aq["high"][3]}) ({total} 个)', callback_data=f'ch:{uid}:audio:high')],
        [InlineKeyboardButton(f'✅ 全部音频 (原质 192k) ({total} 个)', callback_data=f'ch:{uid}:audio:0')],
    ]
    if total <= 50:
        buttons.append([InlineKeyboardButton(
            '🎬 最高画质视频', callback_data=f'ch:{uid}:video:best'
        )])
    # 有播放列表时，加按播放列表下载按钮（4 个音质选项）
    if playlist_count > 0:
        buttons.append([InlineKeyboardButton(
            f'📂 按播放列表 ({aq["low"][3]}) ({playlist_count} 个)',
            callback_data=f'chpl:{uid}:audio:low'
        )])
        buttons.append([InlineKeyboardButton(
            f'📂 按播放列表 ({aq["medium"][3]}) ({playlist_count} 个)',
            callback_data=f'chpl:{uid}:audio:medium'
        )])
        buttons.append([InlineKeyboardButton(
            f'📂 按播放列表 ({aq["high"][3]}) ({playlist_count} 个)',
            callback_data=f'chpl:{uid}:audio:high'
        )])
        buttons.append([InlineKeyboardButton(
            f'📂 按播放列表 (原质 192k) ({playlist_count} 个)',
            callback_data=f'chpl:{uid}:audio:0'
        )])

    await msg.edit_text(
        '\n'.join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='Markdown'
    )


async def callback_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """频道下载回调：按播放列表组织目录结构。"""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(':')
    uid, fmt, res = int(parts[1]), parts[2], parts[3]

    if uid not in playlist_cache:
        await query.edit_message_text('❌ 缓存已过期，请重新发送链接'); return

    cached = playlist_cache[uid]
    info = cached['info']
    playlists = cached.get('playlists', [])
    channel_title = cached.get('channel_title', info.get('title', 'Unknown'))
    channel_safe = re.sub(r'[<>:"/\\|?*]', '_', str(channel_title)).strip().rstrip(' .')
    entries = info['entries']
    total = len(entries)
    loop = asyncio.get_event_loop()

    # chpl: → 按播放列表组织
    if query.data.startswith('chpl:'):
        if not playlists:
            await query.edit_message_text('❌ 未发现播放列表'); return
        fmt_label = '音频 MP3' if fmt == 'audio' else '视频'
        try:
            await query.edit_message_text(
                f"📂 按播放列表下载：*{_esc_md(str(channel_title))}*\n"
                f"共 {len(playlists)} 个播放列表\n"
                f"格式：{fmt_label}\n"
                f"正在逐一下载每个播放列表...",
                parse_mode='Markdown'
            )
        except Exception:
            pass

        total_success, total_failed = 0, 0
        all_failed_items = []
        cancelled = False

        try:
            for pl_idx, pl in enumerate(playlists, 1):
                if cancelled:
                    break

                pl_safe = re.sub(r'[<>:"/\\|?*]', '_', str(pl['title'])).strip().rstrip(' .') or pl['id']
                subdir = os.path.join(channel_safe, pl_safe)

                try:
                    await query.edit_message_text(
                        f"📂 播放列表 {pl_idx}/{len(playlists)}：{_esc_md(str(pl['title']))}\n"
                        f"✅ 已完成：{total_success} | ❌ 失败：{total_failed}",
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass

                try:
                    pl_info = await loop.run_in_executor(None, get_playlist_info, pl['url'])
                except Exception as e:
                    logger.warning(f'获取播放列表失败 {pl["title"]}: {e}')
                    continue

                if not pl_info.get('entries'):
                    continue

                # 获取/创建网站目录结构（频道名 → 播放列表名）
                channel_folder_id = _channel_folder_id_cache.get(channel_title)
                playlist_folder_id = None
                if not channel_folder_id:
                    channel_folder_id = await _ensure_folder(channel_title)
                    if channel_folder_id:
                        # 缓存频道目录 ID，限制缓存大小防止内存泄漏
                        _channel_folder_id_cache[channel_title] = channel_folder_id
                        if len(_channel_folder_id_cache) > 100:
                            # 保留当前条目，只淘汰最旧的
                            _channel_folder_id_cache.clear()
                            _channel_folder_id_cache[channel_title] = channel_folder_id
                if channel_folder_id:
                    playlist_folder_id = await _ensure_folder(pl['title'], parent_id=channel_folder_id)

                for i, entry in enumerate(pl_info['entries'], 1):
                    # 检查取消请求
                    if query.from_user.id in _channel_cancel_reqs:
                        logger.info(f'用户 {query.from_user.id} 取消频道下载（播放列表 {pl["title"]}）')
                        cancelled = True
                        break

                    MAX_RETRIES = 2
                    for attempt in range(MAX_RETRIES + 1):
                        try:
                            if attempt > 0:
                                try:
                                    await query.edit_message_text(
                                        f"🔄 重试 {pl_idx}/{len(playlists)} {i}/{len(pl_info['entries'])} — {_esc_md(str(entry['title'][:35]))}...",
                                        parse_mode='Markdown'
                                    )
                                except Exception:
                                    pass
                                await asyncio.sleep(2)

                            await _rate_limit_youtube()

                            if fmt == 'audio':
                                quality = res if res != '0' else ''
                                meta = await loop.run_in_executor(None, download_audio, entry['url'], subdir, quality)
                            else:
                                meta = await loop.run_in_executor(None, download_video, entry['url'], 'best', subdir)
                            meta['category'] = '油管上传'
                            if playlist_folder_id:
                                meta['folder_id'] = playlist_folder_id

                            logger.info(f'🔧 chpl: 调用 direct_upload: title={meta.get("title","?")}, category={meta.get("category")}, folder_id={meta.get("folder_id")}')
                            result = await direct_upload(meta['file_path'], meta)
                            logger.info(f'🔧 chpl: direct_upload 返回: {result}')
                            try:
                                os.remove(meta['file_path'])
                            except Exception:
                                pass
                            total_success += 1
                            break  # 成功后退出重试循环

                        except Exception as e:
                            logger.warning(f'[{pl["title"]}] 第{i}项失败（第{attempt+1}次）: {e}')
                            if attempt < MAX_RETRIES:
                                await asyncio.sleep(2)
                            else:
                                # 所有重试耗尽，计入失败
                                total_failed += 1
                                all_failed_items.append({'title': entry.get('title', ''), 'url': entry.get('url', '')})

                    # 每项结束后更新进度（更实时）
                    try:
                        await query.edit_message_text(
                            f"📂 播放列表 {pl_idx}/{len(playlists)} {i}/{len(pl_info['entries'])} | "
                            f"✅ {total_success} ❌ {total_failed}\n"
                            f"{_esc_md(str(entry['title'][:40]))}",
                            parse_mode='Markdown'
                        )
                    except Exception:
                        pass  # 编辑失败不级联异常

                    # 每项之间延迟
                    await asyncio.sleep(random.uniform(1, 3))
        finally:
            # 清除取消标志（确保无论异常还是取消都执行）
            _channel_cancel_reqs.discard(query.from_user.id)

        if cancelled:
            result_msg = f'🛑 *频道下载已取消*\n\n'
            result_msg += f"📺 {_esc_md(str(channel_title))}\n"
            result_msg += f"✅ 已完成：{total_success}\n"
            result_msg += f"❌ 失败：{total_failed}\n"
            result_msg += f"⚡ 已处理 {total_success + total_failed} 项"
        else:
            result_msg = f"{'✅' if total_failed == 0 else '⚠️'} *频道按播放列表下载完成*\n\n"
            result_msg += f"📺 {_esc_md(str(channel_title))}\n"
            result_msg += f"✅ 成功：{total_success}\n"
            if total_failed > 0:
                result_msg += f"❌ 失败：{total_failed}"

        await query.edit_message_text(result_msg, parse_mode='Markdown')
        return

    # ch: → 平铺下载
    aq = AUDIO_QUALITY_PRESETS
    quality_label = aq[res][3] if fmt == 'audio' and res in aq else ('原质 192k' if fmt == 'audio' else '')
    fmt_label = f'音频 MP3 ({quality_label})' if fmt == 'audio' else ('视频最高画质' if res == 'best' else f'视频 {res}p')

    # ── 加载续传 checkpoint ──
    channel_key = re.sub(r'[^a-zA-Z0-9]', '_', channel_title)[:50]
    checkpoint = _load_checkpoint(channel_key)
    if checkpoint:
        logger.info(f'⏭️ 找到 checkpoint: {len(checkpoint)} 个视频已处理过，将跳过这些')

    await query.edit_message_text(
        f"⬇️ 开始下载频道：*{_esc_md(str(channel_title))}*\n"
        f"共 {total} 个，格式：{fmt_label}\n"
        f"进度：0/{total}\n"
        f"📂 按频道名组织目录",
        parse_mode='Markdown'
    )

    success, failed = 0, 0
    skipped = 0
    failed_items = []
    cancelled = False
    try:
        for i, entry in enumerate(entries, 1):
            # ══ 续传检查 ══
            entry_vid = _get_video_id_from_url(entry['url'])
            if entry_vid and entry_vid in checkpoint:
                skipped += 1
                logger.info(f'⏭️ {i}/{total} 已处理过，跳过: {entry["title"][:40]}')
                continue

            if i > 1:
                await asyncio.sleep(random.uniform(2, 5))

            # 检查取消请求
            if query.from_user.id in _channel_cancel_reqs:
                logger.info(f'用户 {query.from_user.id} 取消频道下载（ch: 路径）')
                cancelled = True
                break

            MAX_RETRIES = 2
            for attempt in range(MAX_RETRIES + 1):
                try:
                    if attempt > 0:
                        try:
                            await query.edit_message_text(
                                f"🔄 重试 {i}/{total} — {_esc_md(str(entry['title'][:35]))}...",
                                parse_mode='Markdown'
                            )
                        except Exception:
                            pass
                        await asyncio.sleep(2)

                    await _rate_limit_youtube()

                    try:
                        await query.edit_message_text(
                            f"⬇️ 下载 {i}/{total} — {_esc_md(str(entry['title'][:40]))}",
                            parse_mode='Markdown'
                        )
                    except Exception:
                        pass

                    subdir = channel_safe
                    if fmt == 'audio':
                        quality = res if res != '0' else ''
                        meta = await loop.run_in_executor(None, download_audio, entry['url'], subdir, quality)
                    elif res == 'best':
                        meta = await loop.run_in_executor(None, download_video, entry['url'], 'best', subdir)
                    else:
                        meta = await loop.run_in_executor(None, download_video, entry['url'], res, subdir)
                    meta['category'] = '油管上传'

                    logger.info(f'🔧 准备调用 direct_upload: category={meta.get("category")}, folder_id={meta.get("folder_id")}, title={meta.get("title","?")}')
                    result = await direct_upload(meta['file_path'], meta)
                    logger.info(f'🔧 direct_upload 返回: {result}')
                    try:
                        os.remove(meta['file_path'])
                    except Exception:
                        pass
                    # ══ 记录 checkpoint ══
                    if entry_vid:
                        _save_checkpoint(channel_key, entry_vid)
                    success += 1
                    break  # 成功后退出重试循环

                except Exception as e:
                    logger.error(f'频道第{i}项失败（第{attempt+1}次）: {e}')
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(2)
                    else:
                        # 所有重试耗尽，计入失败
                        failed += 1
                        failed_items.append({'title': entry.get('title', ''), 'url': entry.get('url', '')})

            # 每项结束后更新进度（更实时）
            try:
                await query.edit_message_text(
                    f"⬇️ {i}/{total} | ✅ {success} ❌ {failed}\n"
                    f"{_esc_md(str(entry['title'][:40]))}",
                    parse_mode='Markdown'
                )
            except Exception:
                pass
    finally:
        # 清除取消标志（确保无论异常还是取消都执行）
        _channel_cancel_reqs.discard(query.from_user.id)

    if cancelled:
        result_msg = f'🛑 *频道下载已取消*\n\n'
        result_msg += f"📺 {_esc_md(str(channel_title))}\n"
        result_msg += f"✅ 已完成：{success}\n"
        result_msg += f"❌ 失败：{failed}\n"
        result_msg += f"⏭️ 跳过（已处理）：{skipped}\n"
        result_msg += f"⚡ 已处理 {success + failed} 项"
    elif failed == 0:
        # 全部成功 → 清理 checkpoint
        _clear_checkpoint(channel_key)
        result_msg = f"✅ *频道下载完成*\n\n"
        result_msg += f"📺 {_esc_md(str(channel_title))}\n"
        result_msg += f"✅ 成功：{success}\n"
        if skipped:
            result_msg += f"⏭️ 跳过（已处理）：{skipped}\n"
    else:
        result_msg = f"⚠️ *频道下载完成（部分失败）*\n\n"
        result_msg += f"📺 {_esc_md(str(channel_title))}\n"
        result_msg += f"✅ 成功：{success}\n"
        result_msg += f"❌ 失败：{failed}\n"
        if skipped:
            result_msg += f"⏭️ 跳过（已处理）：{skipped}"

    await query.edit_message_text(result_msg, parse_mode='Markdown')

# ──────────────────格式选择──────────────────

async def _show_format_picker(msg, url: str, uid: int):
    await msg.reply_text(f'🔍 获取格式信息...')
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, get_formats, url)
    except Exception as e:
        await msg.reply_text(f'❌ 无法获取格式：{e}'); return

    _cache_put(format_cache, uid, {'url': url, 'info': info})

    aq = AUDIO_QUALITY_PRESETS
    buttons = [
        [InlineKeyboardButton(f'🎵 MP3 {aq["low"][3]}', callback_data=f'fmt:{uid}:audio:low')],
        [InlineKeyboardButton(f'🎵 MP3 {aq["medium"][3]}', callback_data=f'fmt:{uid}:audio:medium')],
        [InlineKeyboardButton(f'🎵 MP3 {aq["high"][3]}', callback_data=f'fmt:{uid}:audio:high')],
        [InlineKeyboardButton('🎵 MP3 原质 192k', callback_data=f'fmt:{uid}:audio:0')],
    ]
    for vf in info['video_formats']:
        buttons.append([InlineKeyboardButton(
            f'🎬 视频 {_height_label(vf["height"])}',
            callback_data=f'fmt:{uid}:video:{vf["format_id"]}'
        )])

    await msg.reply_text(
        f"🎵 *{_esc_md(str(info['title']))}*\n⏱ {fmt_dur(info['duration'])}\n\n请选择下载格式：",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='Markdown'
    )

async def callback_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, uid, idx = query.data.split(':')
    uid, idx = int(uid), int(idx)
    if uid not in search_cache or idx >= len(search_cache[uid]):
        await query.edit_message_text('❌ 请重新搜索'); return
    item = search_cache[uid][idx]
    await query.edit_message_text(f'🔍 获取「{item["title"]}」格式信息...')
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, get_formats, item['url'])
    except Exception as e:
        await query.edit_message_text(f'❌ 无法获取格式：{e}'); return

    _cache_put(format_cache, uid, {'url': item['url'], 'info': info})

    aq = AUDIO_QUALITY_PRESETS
    buttons = [
        [InlineKeyboardButton(f'🎵 MP3 {aq["low"][3]}', callback_data=f'fmt:{uid}:audio:low')],
        [InlineKeyboardButton(f'🎵 MP3 {aq["medium"][3]}', callback_data=f'fmt:{uid}:audio:medium')],
        [InlineKeyboardButton(f'🎵 MP3 {aq["high"][3]}', callback_data=f'fmt:{uid}:audio:high')],
        [InlineKeyboardButton('🎵 MP3 原质 192k', callback_data=f'fmt:{uid}:audio:0')],
    ]
    for vf in info['video_formats']:
        buttons.append([InlineKeyboardButton(
            f'🎬 视频 {_height_label(vf["height"])}',
            callback_data=f'fmt:{uid}:video:{vf["format_id"]}'
        )])

    await query.edit_message_text(
        f"🎵 *{_esc_md(str(info['title']))}*\n⏱ {fmt_dur(info['duration'])}\n\n请选择下载格式：",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='Markdown'
    )

async def callback_format(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(':')
    uid, fmt, fmt_id = int(parts[1]), parts[2], parts[3]

    if uid not in format_cache:
        await query.edit_message_text('❌ 缓存已过期，请重新搜索'); return

    cached = format_cache[uid]
    url = cached['url']
    info = cached['info']

    # 解析音质参数（audio: low/medium/high/0）
    quality = fmt_id if fmt == 'audio' else None
    fmt_label = '音频 MP3'
    if fmt == 'audio':
        aq = AUDIO_QUALITY_PRESETS
        q_label = aq[quality][3] if quality and quality in aq else '原质 192k'
        fmt_label = f'音频 MP3 ({q_label})'
    else:
        fmt_label = f'视频 {fmt_id}p'

    await query.edit_message_text(
        f"⬇️ 下载中：{fmt_label}\n🎵 {info['title']}..."
    )
    await _do_download_and_upload(query.message, url, {}, fmt, fmt_id if fmt == 'video' else None, quality if fmt == 'audio' else None)

async def _pipeline_process_entries(
    entries: list,
    fmt: str,
    res: str,
    subdir_fn,
    progress_cb,
    loop,
    category: str = '油管上传',
) -> tuple:
    """
    管道处理：下载 N+1 的同时上传 N，任何时候磁盘上最多 2 个文件。

    Args:
        entries: [{url, title, ...}]
        fmt: 'audio' / 'video'
        res: format_id 或 'best'
        subdir_fn: callable(entry, idx) → subdir 字符串
        progress_cb: callable(success, failed, current, total) → None
        loop: asyncio 事件循环
        category: 入库分类

    Returns:
        (success, failed, failed_items)
    """
    n = len(entries)
    if n == 0:
        return 0, 0, []

    success, failed = 0, 0
    failed_items = []

    _MAX_RETRIES = 1  # 下载失败自动重试一次

    # ── 单个任务的下载协程工厂（含格式发现和重试）──
    async def _dl(entry, idx):
        subdir = subdir_fn(entry, idx) if subdir_fn else ''
        if fmt == 'audio':
            for attempt in range(_MAX_RETRIES + 1):
                try:
                    return await loop.run_in_executor(None, download_audio, entry['url'], subdir)
                except Exception as e:
                    if attempt < _MAX_RETRIES:
                        logger.warning(f'音频下载重试 #{idx}: {e}')
                        await asyncio.sleep(2)
                    else:
                        raise
        else:
            # 视频格式：非预设分辨率时需要先 get_formats 找 format_id
            if res in ('best', '1080', '720') or res.startswith('best'):
                fmt_id = res
            else:
                fmt_id = res
                try:
                    fmts = await loop.run_in_executor(None, get_formats, entry['url'])
                    target_h = int(res)
                    matched = next((f for f in fmts['video_formats'] if f['height'] == target_h), None)
                    if matched:
                        fmt_id = matched['format_id']
                except Exception:
                    pass
            for attempt in range(_MAX_RETRIES + 1):
                try:
                    return await loop.run_in_executor(None, download_video, entry['url'], fmt_id, subdir)
                except Exception as e:
                    if attempt < _MAX_RETRIES:
                        logger.warning(f'视频下载重试 #{idx}: {e}')
                        await asyncio.sleep(2)
                    else:
                        raise

    # 预启动第一个下载
    download_task = asyncio.create_task(_dl(entries[0], 0))

    for i in range(n):
        entry = entries[i]

        # 1. 等待当前下载完成（含重试）
        try:
            meta = await download_task
        except Exception as e:
            logger.warning(f'第{i+1}/{n}项下载失败: {e}')
            failed += 1
            failed_items.append({'title': entry.get('title', ''), 'url': entry.get('url', '')})
            # 预启动下一个
            if i + 1 < n:
                download_task = asyncio.create_task(_dl(entries[i + 1], i + 1))
            await progress_cb(success, failed, i + 1, n)
            if i + 1 < n:
                await asyncio.sleep(random.uniform(1, 3))
            continue

        # 2. 预启动下一个下载（管道核心：下载与上传并行）
        if i + 1 < n:
            download_task = asyncio.create_task(_dl(entries[i + 1], i + 1))

        # 3. 上传当前
        meta['category'] = category
        try:
            result = await direct_upload(meta['file_path'], meta)
            success += 1
        except Exception as e:
            logger.warning(f'第{i+1}/{n}项上传失败: {e}')
            failed += 1
            failed_items.append({'title': entry.get('title', ''), 'url': entry.get('url', '')})

        # 4. 立即删除当前
        try:
            os.remove(meta['file_path'])
        except Exception:
            pass

        await progress_cb(success, failed, i + 1, n)

        if i + 1 < n:
            await asyncio.sleep(random.uniform(1, 3))

    return success, failed, failed_items


async def _process_playlist_entries(query, info, entries, total, fmt, res):
    """处理播放列表条目，带重试逻辑和失败收集。返回 (success, failed, failed_items)。"""
    success, failed = 0, 0
    failed_items = []
    loop = asyncio.get_event_loop()

    for i, entry in enumerate(entries, 1):
        # 每项之间延迟 2-5 秒，避免触发 flood control
        if i > 1:
            await asyncio.sleep(random.uniform(2, 5))

        MAX_RETRIES = 1
        for attempt in range(MAX_RETRIES + 1):
            try:
                if attempt > 0:
                    await query.edit_message_text(
                        f"🔄 重试 {i}/{total} — {_esc_md(str(entry['title'][:35]))}...\n📋 {_esc_md(str(info['title']))}",
                        parse_mode='Markdown'
                    )
                    await asyncio.sleep(2)

                await query.edit_message_text(
                    f"⬇️ {'重试' if attempt > 0 else '下载'}中 {i}/{total} — {_esc_md(str(entry['title'][:40]))}\n📋 {_esc_md(str(info['title']))}",
                    parse_mode='Markdown'
                )

                if fmt == 'audio':
                    quality = res if res != '0' else ''
                    meta = await loop.run_in_executor(None, download_audio, entry['url'], '', quality)
                elif res == 'best':
                    # 最高画质：跳过 get_formats，直接 bestvideo+bestaudio
                    meta = await loop.run_in_executor(None, download_video, entry['url'], 'best')
                else:
                    fmts = await loop.run_in_executor(None, get_formats, entry['url'])
                    target_h = int(res)
                    matched = next((f for f in fmts['video_formats'] if f['height'] == target_h), None)
                    if not matched:
                        matched = min(fmts['video_formats'], key=lambda f: abs(f['height'] - target_h), default=None)
                    if not matched:
                        failed_items.append({'url': entry['url'], 'title': entry.get('title', '')})
                        failed += 1
                        break
                    meta = await loop.run_in_executor(None, download_video, entry['url'], matched['format_id'])

                result = await direct_upload(meta['file_path'], meta)
                try:
                    os.remove(meta['file_path'])
                except Exception:
                    pass
                success += 1
                break  # 成功后退出重试循环
            except Exception as e:
                logger.error(f'播放列表第{i}项失败（第{attempt+1}次）：{e}')
                if attempt < MAX_RETRIES:
                    logger.info(f'即将重试第{i}项...')
                else:
                    failed_items.append({'url': entry['url'], 'title': entry.get('title', '')})
                    failed += 1

    return success, failed, failed_items


async def callback_playlist(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(':')
    uid, fmt, res = int(parts[1]), parts[2], parts[3]

    if uid not in playlist_cache:
        await query.edit_message_text('❌ 缓存已过期，请重新发送链接'); return

    info = playlist_cache[uid]
    entries = info['entries']
    total = len(entries)

    aq = AUDIO_QUALITY_PRESETS
    quality_label = aq[res][3] if fmt == 'audio' and res in aq else ('原质 192k' if fmt == 'audio' else '')
    fmt_label = f'音频 MP3 ({quality_label})' if fmt == 'audio' else ('视频最高画质' if res == 'best' else f'视频 {res}p')
    await query.edit_message_text(
        f"⬇️ 开始下载播放列表：*{_esc_md(str(info['title']))}*\n"
        f"共 {total} 个，格式：{fmt_label}\n"
        f"进度：0/{total}",
        parse_mode='Markdown'
    )

    success, failed, failed_items = await _process_playlist_entries(
        query, info, entries, total, fmt, res
    )

    # 构建最终消息
    result_msg = f"{'✅' if failed == 0 else '⚠️'} *播放列表下载完成*\n\n"
    result_msg += f"📋 {_esc_md(str(info['title']))}\n"
    result_msg += f"✅ 成功：{success}\n"
    if failed > 0:
        result_msg += f"❌ 失败：{failed}\n"
        # 列出失败项标题（最多显示 5 个，避免消息过长）
        for item in failed_items[:5]:
            result_msg += f"   • {_esc_md(str(item['title'][:40]))}\n"
        if len(failed_items) > 5:
            result_msg += f'   … 还有 {len(failed_items) - 5} 项\n'

    buttons = []
    if failed > 0:
        # 保存失败项到缓存，供重试按钮使用
        if 'failed_items' not in playlist_cache:
            _cache_put(playlist_cache, 'failed_items', {})
        playlist_cache['failed_items'][uid] = {'fmt': fmt, 'res': res, 'items': failed_items}
        buttons.append([InlineKeyboardButton(
            f'🔄 重试失败项 ({failed})',
            callback_data=f'rpl:{uid}:{fmt}:{res}'
        )])

    await query.edit_message_text(
        result_msg,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
        parse_mode='Markdown'
    )


async def callback_retry_playlist_failed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """重试播放列表中失败的项。"""
    query = update.callback_query
    await query.answer()
    parts = query.data.split(':')
    uid, fmt, res = int(parts[1]), parts[2], parts[3]

    if 'failed_items' not in playlist_cache or uid not in playlist_cache['failed_items']:
        await query.edit_message_text('❌ 缓存已过期，请重新发送链接'); return

    cached = playlist_cache['failed_items'][uid]
    failed_entries = cached['items']
    total = len(failed_entries)
    title = '重试失败项'

    aq = AUDIO_QUALITY_PRESETS
    rquality_label = aq[res][3] if fmt == 'audio' and res in aq else ('原质 192k' if fmt == 'audio' else '')
    rfmt_label = f'音频 MP3 ({rquality_label})' if fmt == 'audio' else ('视频最高画质' if res == 'best' else f'视频 {res}p')
    await query.edit_message_text(
        f"🔄 开始重试 {total} 个失败项...\n"
        f"格式：{rfmt_label}\n"
        f"进度：0/{total}",
        parse_mode='Markdown'
    )

    # 复用处理逻辑，把失败项当作 entries
    success, failed, failed_items2 = await _process_playlist_entries(
        query, {'title': title}, failed_entries, total, fmt, res
    )

    result_msg = f"{'✅' if failed == 0 else '⚠️'} *重试完成*\n\n"
    result_msg += f"📋 原播放列表\n"
    result_msg += f"✅ 成功：{success}\n"
    if failed > 0:
        result_msg += f"❌ 失败：{failed}\n"
        for item in failed_items2[:5]:
            result_msg += f"   • {_esc_md(str(item['title'][:40]))}\n"
        if len(failed_items2) > 5:
            result_msg += f'   … 还有 {len(failed_items2) - 5} 项\n'

    buttons = []
    if failed > 0:
        playlist_cache['failed_items'][uid] = {'fmt': fmt, 'res': res, 'items': failed_items2}
        buttons.append([InlineKeyboardButton(
            f'🔄 再次重试 ({failed})',
            callback_data=f'rpl:{uid}:{fmt}:{res}'
        )])

    await query.edit_message_text(
        result_msg,
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
        parse_mode='Markdown'
    )

# ── 目录管理（获取/创建网站目录）──

async def _ensure_folder(name: str, parent_id: int = None) -> int | None:
    """获取或创建网站目录，返回 folder_id。失败返回 None。"""
    headers = {'X-Admin-Token': config.CF_API_KEY, 'Content-Type': 'application/json'}
    if not config.CF_WORKER_URL:
        return None
    try:
        # 1. 查找已存在的目录
        async with aiohttp.ClientSession() as session:
            params = {}
            if parent_id is not None:
                params['parent_id'] = str(parent_id)
            async with session.get(
                f"{config.CF_WORKER_URL}/api/admin/folders",
                headers=headers, params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    folders = await resp.json()
                    if isinstance(folders, list):
                        for f in folders:
                            if f.get('name') == name and f.get('parent_id') == parent_id:
                                return f['id']

        # 2. 不存在 → 创建
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{config.CF_WORKER_URL}/api/admin/folders",
                headers=headers,
                json={"name": name, "parent_id": parent_id, "sort_order": 0},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('id')
                # 409 = 同级已存在同名目录，可能由并发创建导致
                if resp.status == 409:
                    data = await resp.json()
                    return data.get('duplicate_id')
    except Exception as e:
        logger.warning(f'目录操作失败 ({name}): {e}')
    return None


# ──────────────────核心逻辑──────────────────

async def _do_download_and_upload(msg, url: str, metadata: dict, fmt: str, fmt_id, quality: str = None):
    loop = asyncio.get_event_loop()
    try:
        if fmt == 'video':
            await msg.edit_text('⬇️ 正在下载视频...')
            meta = await loop.run_in_executor(None, download_video, url, fmt_id)
        else:
            q_text = f'（{AUDIO_QUALITY_PRESETS[quality][3]}）' if quality and quality in AUDIO_QUALITY_PRESETS else ''
            await msg.edit_text(f'⬇️ 正在下载音频 {q_text}...')
            meta = await loop.run_in_executor(None, download_audio, url, '', quality or '')

        meta.update({k: v for k, v in metadata.items() if v})

        size_mb = os.path.getsize(meta['file_path']) / 1024 / 1024
        await msg.edit_text(
            f"📤 上传中（{size_mb:.1f} MB）\n"
            f"🎵 {meta['title']}\n"
            f"📂 {meta.get('category', config.DEFAULT_CATEGORY)}"
        )

        result = await direct_upload(meta['file_path'], meta)

        try:
            os.remove(meta['file_path'])
        except Exception:
            pass

        await msg.edit_text(
            f"✅ *上传成功！*\n\n"
            f"🎵 {_esc_md(str(meta['title']))}\n"
            f"📂 {_esc_md(str(meta.get('category', config.DEFAULT_CATEGORY)))}\n"
            f"⏱ {fmt_dur(meta.get('duration'))}\n"
            f"🆔 ID：`{_esc_md(str(result.get('id', '?')))}`",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.exception('失败')
        try:
            await msg.edit_text(f'❌ 处理失败：{str(e)[:200]}')
        except Exception:
            pass

# ──────────────────Worker 任务轮询──────────────────

def _worker_headers() -> dict:
    return {'X-Admin-Token': config.CF_API_KEY, 'Content-Type': 'application/json'}

async def _patch_task(session: aiohttp.ClientSession, task_id: int, **kwargs) -> bool:
    """更新任务状态/进度。返回 True 表示 Worker 返回 200，False 表示失败。"""
    url = f"{config.CF_WORKER_URL}/api/bot/tasks/{task_id}"
    try:
        async with session.patch(url, json=kwargs, headers=_worker_headers()) as r:
            if r.status == 200:
                return True
            logger.warning(f'[{config.BOT_ID}] patch task {task_id} failed: {r.status}')
    except Exception as e:
        logger.warning(f'[{config.BOT_ID}] patch task {task_id} error: {e}')
    return False


async def _patch_task_retry(session: aiohttp.ClientSession, task_id: int, **kwargs) -> bool:
    """带重试的 _patch_task，最多重试 3 次（1s/2s/4s 退避）。
    仅用于关键状态更新（done/failed），确保不会因 Worker 临时故障导致任务卡死。"""
    max_retries = 3
    for attempt in range(max_retries):
        ok = await _patch_task(session, task_id, **kwargs)
        if ok:
            return True
        if attempt < max_retries - 1:
            delay = 2 ** attempt  # 1, 2, 4 秒
            logger.warning(f'[{config.BOT_ID}] _patch_task_retry #{task_id} {delay}s 后第{attempt+2}次重试...')
            await asyncio.sleep(delay)
    logger.error(f'[{config.BOT_ID}] _patch_task_retry #{task_id} 重试{max_retries}次后仍失败')
    return False

async def _execute_task(session: aiohttp.ClientSession, task: dict):
    task_id = task['id']
    url = task['url']
    mode = task.get('mode', 'audio')
    fmt = task.get('format')
    category = task.get('category') or config.DEFAULT_CATEGORY

    logger.info(f'[{config.BOT_ID}] 开始任务 #{task_id}: {mode} {url}')
    loop = asyncio.get_event_loop()

    import json

    # 每个任务开始时应用速率限制（非播放列表单次请求），防止连续触发 YouTube 限流
    await _rate_limit_youtube()

    try:
        # ── 播放列表模式：解析列表，逐项下载 ──
        if mode == 'playlist':
            await _patch_task(session, task_id, status='processing', progress='正在解析播放列表...')
            try:
                info = await loop.run_in_executor(None, get_playlist_info, url)
            except Exception as e:
                await _patch_task_retry(session, task_id, status='failed', error=f'解析播放列表失败：{e}')
                logger.error(f'[{config.BOT_ID}] 播放列表解析失败 #{task_id}: {e}')
                return

            entries = info.get('entries', [])
            total = len(entries)
            if total == 0:
                await _patch_task_retry(session, task_id, status='failed', error='播放列表为空')
                return

            success, failed = 0, 0
            items = []
            failed_items = []
            cancelled = False
            for i, entry in enumerate(entries, 1):
                # 每项开始前检查是否被取消（含第 1 项）
                try:
                    async with session.get(
                        f"{config.CF_WORKER_URL}/api/bot/tasks/{task_id}/status",
                        headers=_worker_headers()
                    ) as sr:
                        if sr.status == 200:
                            sd = await sr.json()
                            if sd.get('status') == 'cancelled':
                                logger.info(f'[{config.BOT_ID}] 播放列表 #{task_id} 已被用户取消（第{i}项）')
                                cancelled = True
                                break
                except Exception:
                    pass  # 检查失败时继续处理，不阻塞下载

                # 每项开始前应用 YouTube 速率限制
                await _rate_limit_youtube()

                # 每项之间额外延迟 2-5 秒，避免触发频率限制
                if i > 1:
                    await asyncio.sleep(random.uniform(2, 5))

                # 失败后自动重试一次
                MAX_RETRIES = 1
                for attempt in range(MAX_RETRIES + 1):
                    try:
                        if attempt > 0:
                            retry_msg = f'🔄 重试 {i}/{total} — {entry["title"][:35]}...'
                            await _patch_task(session, task_id, progress=retry_msg)
                            await asyncio.sleep(2)

                        progress_msg = f'下载中 {i}/{total} — {entry["title"][:40]}'
                        await _patch_task(session, task_id, progress=progress_msg)

                        if fmt:
                            # 视频模式（fmt 可能是 'best'/'1080'/'720'，download_video 内部处理）
                            meta = await loop.run_in_executor(None, download_video, entry['url'], fmt)
                        else:
                            # 音频模式
                            meta = await loop.run_in_executor(None, download_audio, entry['url'])

                        meta['category'] = category
                        folder_id = task.get('folder_id')
                        if folder_id is not None:
                            meta['folder_id'] = folder_id

                        upload_msg = f'上传中 {i}/{total} — {meta.get("title", entry["title"])[:30]}...'
                        await _patch_task(session, task_id, progress=upload_msg)
                        result = await direct_upload(meta['file_path'], meta, uploader_id=task.get('user_id'))

                        try:
                            os.remove(meta['file_path'])
                        except Exception:
                            pass

                        success += 1
                        items.append({'id': result.get('id'), 'title': meta.get('title', '')})
                        break  # 成功后退出重试循环
                    except Exception as e:
                        logger.error(f'[{config.BOT_ID}] 播放列表第{i}项失败（第{attempt+1}次） #{task_id}: {e}')
                        if attempt < MAX_RETRIES:
                            logger.info(f'[{config.BOT_ID}] 即将重试第{i}项...')
                        else:
                            failed_items.append({'url': entry['url'], 'title': entry.get('title', '')})
                            failed += 1

            if cancelled:
                summary = f'已取消：成功 {success}，失败 {failed}，已完成 {success+failed}/{total}'
                result_data = {
                    'success': success,
                    'failed': failed,
                    'total': total,
                    'cancelled': True,
                    'playlist_title': info.get('title', ''),
                    'items': items,
                    'failed_items': failed_items,
                }
                # 不传 status——cancel 端点已将其设置为 'cancelled'；worker 的 PATCH 也不允许 bot 设置 'cancelled'
                await _patch_task(
                    session, task_id,
                    progress=summary,
                    title=info.get('title', ''),
                    result=json.dumps(result_data, ensure_ascii=False)
                )
                logger.info(f'[{config.BOT_ID}] 播放列表任务 #{task_id} 已取消: {summary}')
            else:
                summary = f'完成：成功 {success}，失败 {failed}，共 {total}'
                result_data = {
                    'success': success,
                    'failed': failed,
                    'total': total,
                    'playlist_title': info.get('title', ''),
                    'items': items,
                    'failed_items': failed_items,
                }
                await _patch_task_retry(
                    session, task_id,
                    status='done',
                    progress=summary,
                    title=info.get('title', ''),
                    result=json.dumps(result_data, ensure_ascii=False)
                )
                logger.info(f'[{config.BOT_ID}] 播放列表任务 #{task_id} 完成: {summary}')
            return

        # ── youtube_direct 模式：下载到磁盘，不上传 Telegram，用户自行下载 ──
        if mode == 'youtube_direct':
            await _patch_task(session, task_id, status='processing', progress='下载中...')
            await asyncio.sleep(random.uniform(1.0, 3.0))
            try:
                if fmt and fmt != 'audio':
                    meta = await loop.run_in_executor(None, download_video, url, fmt)
                else:
                    meta = await loop.run_in_executor(None, download_audio, url)
            except Exception as e:
                await _patch_task_retry(session, task_id, status='failed', error=f'下载失败：{str(e)[:300]}')
                logger.exception(f'[{config.BOT_ID}] youtube_direct 任务 #{task_id} 下载失败')
                return

            file_name = os.path.basename(meta['file_path'])
            file_size = os.path.getsize(meta['file_path'])
            logger.info(f'[{config.BOT_ID}] youtube_direct 任务 #{task_id} 下载完成: {file_name} ({file_size/1024/1024:.1f} MB)')

            # 不上传 Telegram，保留文件在磁盘上由 _cleanup_temp_dir 清理
            await _patch_task_retry(
                session, task_id,
                status='done',
                progress='完成',
                title=meta.get('title', ''),
                result=json.dumps({
                    'file_name': file_name,
                    'file_size': file_size,
                    'title': meta.get('title', ''),
                }, ensure_ascii=False)
            )
            return

        # ── youtube_direct_tg 模式：下载后上传到 Telegram CDN，入库到诗歌本 ──
        if mode == 'youtube_direct_tg':
            await _patch_task(session, task_id, status='processing', progress='下载中...')
            await asyncio.sleep(random.uniform(1.0, 3.0))
            try:
                if fmt and fmt != 'audio':
                    meta = await loop.run_in_executor(None, download_video, url, fmt)
                else:
                    meta = await loop.run_in_executor(None, download_audio, url)
            except Exception as e:
                await _patch_task_retry(session, task_id, status='failed', error=f'下载失败：{str(e)[:300]}')
                logger.exception(f'[{config.BOT_ID}] youtube_direct_tg 任务 #{task_id} 下载失败')
                return

            meta['category'] = category
            folder_id = task.get('folder_id')
            if folder_id is not None:
                meta['folder_id'] = folder_id

            size_mb = os.path.getsize(meta['file_path']) / 1024 / 1024
            await _patch_task(session, task_id, progress=f'上传到 Telegram CDN（{size_mb:.1f} MB）...')

            try:
                result = await direct_upload(meta['file_path'], meta, uploader_id=task.get('user_id'), skip_import=True)
            except Exception as e:
                try:
                    os.remove(meta['file_path'])
                except Exception:
                    pass
                await _patch_task_retry(session, task_id, status='failed', error=f'上传失败：{str(e)[:300]}')
                logger.exception(f'[{config.BOT_ID}] youtube_direct_tg 任务 #{task_id} 上传失败')
                return

            try:
                os.remove(meta['file_path'])
            except Exception:
                pass

            await _patch_task_retry(
                session, task_id,
                status='done',
                progress='完成',
                title=meta.get('title', ''),
                result=json.dumps({
                    'file_parts': result.get('file_parts'),
                    'file_name': result.get('file_name', ''),
                    'file_size': result.get('file_size', 0),
                    'title': meta.get('title', ''),
                    'duration': meta.get('duration', 0),
                    'mime_type': result.get('mime_type', 'audio/mpeg'),
                }, ensure_ascii=False)
            )
            logger.info(f'[{config.BOT_ID}] youtube_direct_tg 任务 #{task_id} 完成，上传到 Telegram CDN')
            return

        # ── 非播放列表：单个文件下载 ──
        # 检查该视频是否在限流冷却中
        rl_msg = _check_yt_rate_limit(url)
        if rl_msg:
            raise Exception(rl_msg)
        # 单个文件下载也添加随机延迟，避免短时间内连发
        await asyncio.sleep(random.uniform(1.0, 3.0))
        await _patch_task(session, task_id, status='processing', progress='下载中...')
        if mode == 'video':
            if not fmt:
                fmt = 'bestvideo+bestaudio'
            meta = await loop.run_in_executor(None, download_video, url, fmt)
        else:
            meta = await loop.run_in_executor(None, download_audio, url)

        meta['category'] = category
        folder_id = task.get('folder_id')
        if folder_id is not None:
            meta['folder_id'] = folder_id
        size_mb = os.path.getsize(meta['file_path']) / 1024 / 1024
        await _patch_task(session, task_id, progress=f'上传中（{size_mb:.1f} MB）...')

        result = await direct_upload(meta['file_path'], meta, uploader_id=task.get('user_id'))

        try:
            os.remove(meta['file_path'])
        except Exception:
            pass

        await _patch_task_retry(
            session, task_id,
            status='done',
            progress='完成',
            title=meta.get('title', ''),
            result=json.dumps({'id': result.get('id'), 'title': meta.get('title', '')}, ensure_ascii=False)
        )
        logger.info(f'[{config.BOT_ID}] 任务 #{task_id} 完成，hymn_id={result.get("id")}')
    except Exception as e:
        error_str = str(e)
        # ── 检测 YouTube 限流 → 递增重试（全部耗尽后才标记缓存）──
        is_rate_limit = any(k in error_str.lower() for k in ['rate-limited', 'rate_limit', '429', 'too many requests'])

        if is_rate_limit:
            max_retries = 3
            for retry in range(max_retries):
                base_wait = 60 * (retry + 1)
                wait_time = random.uniform(base_wait, base_wait * 1.5)
                logger.warning(
                    f'[{config.BOT_ID}] 任务 #{task_id} 被 YouTube 限流，'
                    f'第 {retry+1}/{max_retries} 次重试，等待 {wait_time:.0f} 秒...'
                )
                await _patch_task(
                    session, task_id,
                    progress=f'⚠️ 被限流，第 {retry+1}/{max_retries} 次重试（等待 {wait_time:.0f} 秒）...'
                )
                await asyncio.sleep(wait_time)
                # 重置请求间隔计时器
                global _last_youtube_request
                async with _youtube_request_lock:
                    _last_youtube_request = 0.0
                # 再次尝试
                try:
                    await _execute_task(session, task)
                    return
                except Exception as e2:
                    e2_str = str(e2)
                    if any(k in e2_str.lower() for k in ['rate-limited', 'rate_limit', '429', 'too many requests']):
                        logger.warning(
                            f'[{config.BOT_ID}] 任务 #{task_id} 第 {retry+1} 次重试后仍限流'
                        )
                        continue
                    else:
                        error_str = e2_str
                        break
            else:
                # 所有重试耗尽 → 标记限流缓存 + 通知管理员
                _mark_yt_rate_limit(url)
                task_title = task.get('title', '') or ''
                video_id = _get_video_id(url)
                await _alert_admin(
                    f'🚫 YouTube 限流（{config.BOT_ID}）\n'
                    f'任务 #{task_id}：{task_title[:40]}\n'
                    f'video_id：{video_id}\n'
                    f'已冷却 {_YT_RATE_LIMIT_COOLDOWN // 60} 分钟'
                )

        logger.exception(f'[{config.BOT_ID}] 任务 #{task_id} 失败')
        if 'meta' in locals() and meta and 'file_path' in meta:
            try:
                os.remove(meta['file_path'])
            except Exception:
                pass
        await _patch_task_retry(session, task_id, status='failed', error=error_str[:500])

async def _execute_task_with_semaphore(session, task):
    async with _task_semaphore:
        await _execute_task(session, task)


async def _task_poller():
    if not config.CF_WORKER_URL or not config.CF_API_KEY:
        logger.warning(f'[{config.BOT_ID}] CF_WORKER_URL 或 CF_API_KEY 未配置，任务轮询已禁用')
        return

    logger.info(f'[{config.BOT_ID}] 任务轮询已启动，间隔 {config.POLL_INTERVAL}s')
    poll_url = f"{config.CF_WORKER_URL}/api/bot/tasks/poll"

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # 如果当前 Bot 正在忙（信号量被占用），不轮询新任务，防止单 Bot 囤积
                if _task_semaphore.locked():
                    await asyncio.sleep(2)
                    continue

                async with session.post(
                    poll_url,
                    json={'bot_id': config.BOT_ID},
                    headers=_worker_headers()
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        task = data.get('task')
                        if task:
                            # 用信号量限制并发，最多同时处理 1 个任务
                            asyncio.create_task(_execute_task_with_semaphore(session, task))
                        # 随机 jitter 避免 5 个 bot 同时轮询
                        base_sleep = 0.5 if task else config.POLL_INTERVAL
                        await asyncio.sleep(base_sleep + random.uniform(0, 1.5))
                    else:
                        logger.warning(f'[{config.BOT_ID}] poll 失败: {resp.status}')
                        await asyncio.sleep(config.POLL_INTERVAL)
            except Exception as e:
                logger.warning(f'[{config.BOT_ID}] 轮询异常: {e}')
                await asyncio.sleep(config.POLL_INTERVAL)

# ──────────────────HTTP 搜索服务（8080，仅 bot0）──────────────────

async def _verify_jwt_with_backend(token: str) -> bool:
    if not config.CF_WORKER_URL or not token:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.CF_WORKER_URL}/api/admin/me",
                headers={'Authorization': f'Bearer {token}'},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                return resp.status == 200
    except Exception as e:
        logger.warning(f'验证 JWT 异常: {e}')
        return False

async def _verify_ticket_with_backend(ticket: str) -> dict | None:
    """向 Worker 验证一次性下载凭证。返回验证通过的参数 dict，失败返回 None。"""
    if not config.CF_WORKER_URL or not ticket:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{config.CF_WORKER_URL}/api/youtube-dl/verify-ticket",
                json={'ticket': ticket},
                headers={'X-Admin-Token': config.CF_API_KEY, 'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('valid'):
                        return data
        return None
    except Exception as e:
        logger.warning(f'验证 ticket 异常: {e}')
        return None


async def _check_search_auth(request: aiohttp_web.Request) -> bool:
    if request.headers.get('X-Admin-Token', '') == config.CF_API_KEY:
        return True
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        token = auth[7:].strip()
        return await _verify_jwt_with_backend(token)
    # 支持从 query parameter 传递一次性下载凭证（替代已移除的 ?token=）
    query_ticket = request.rel_url.query.get('ticket', '')
    if query_ticket:
        result = await _verify_ticket_with_backend(query_ticket)
        if result:
            # 将验证通过的参数存入 request，供下载处理使用
            request._ticket_data = result
            return True
    return False

def _get_video_id(url: str) -> str:
    """从 YouTube URL 提取视频 ID"""
    m = re.search(r'(?:v=|youtu\.be/|shorts/)([a-zA-Z0-9_-]{11})', url)
    return m.group(1) if m else ''


async def _handle_search_http(request: aiohttp_web.Request):
    if not await _check_search_auth(request):
        return aiohttp_web.Response(status=401, text='Unauthorized')
    q = request.rel_url.query.get('q', '').strip()
    max_r = min(int(request.rel_url.query.get('max', '8')), 10)
    if len(q) < 2:
        return aiohttp_web.json_response({'results': []})
    loop = asyncio.get_event_loop()
    try:
        results = await loop.run_in_executor(None, search_youtube, q, max_r)
        return aiohttp_web.json_response({'results': [
            {
                'url':       r['url'],
                'title':     r['title'],
                'uploader':  r.get('uploader', ''),
                'duration':  r.get('duration', 0),
                'thumbnail': r.get('thumbnail', ''),
            }
            for r in results
        ]})
    except Exception as e:
        logger.error(f'HTTP 搜索异常: {e}')
        return aiohttp_web.json_response({'error': str(e)}, status=500)


async def _handle_formats_http(request: aiohttp_web.Request):
    """返回 YouTube 视频的可用格式列表（含分辨率）。"""
    if not await _check_search_auth(request):
        return aiohttp_web.Response(status=401, text='Unauthorized')

    url = request.rel_url.query.get('url', '').strip()
    if not url:
        return aiohttp_web.json_response({'error': '缺少 url 参数'}, status=400)

    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, get_formats, url)
        return aiohttp_web.json_response({
            'title':    info['title'],
            'duration': info['duration'],
            'uploader': info['uploader'],
            'formats':  info['video_formats'],
        })
    except Exception as e:
        logger.error(f'获取格式异常: {e}')
        return aiohttp_web.json_response({'error': str(e)}, status=500)

async def _handle_download_file_http(request: aiohttp_web.Request):
    """提供已缓存文件的直接下载（youtube_direct 模式）。
    文件已在磁盘上，响应即时，不存在 yt-dlp 长时间预处理的问题。
    """
    if not await _check_search_auth(request):
        return aiohttp_web.Response(status=401, text='Unauthorized')

    # 从 ticket 验证结果中获取 task_id
    task_id = None
    if hasattr(request, '_ticket_data') and request._ticket_data:
        td = request._ticket_data
        # 只处理 file_download 类型的 ticket
        if td.get('format') == 'file_download':
            try:
                task_id = int(td.get('url', '0'))
            except (ValueError, TypeError):
                pass

    if not task_id:
        return aiohttp_web.Response(
            status=403,
            text='无效的下载凭证',
            content_type='text/plain; charset=utf-8'
        )

    # 从 Worker API 获取任务详情，提取 file_name
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.CF_WORKER_URL}/api/bot/tasks/{task_id}/result-detail",
                headers={'X-Admin-Token': config.CF_API_KEY},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.warning(f'获取任务详情失败: {resp.status} task_id={task_id}')
                    return aiohttp_web.Response(text='获取任务详情失败', status=502, content_type='text/plain; charset=utf-8')
                task_data = await resp.json()
    except Exception as e:
        logger.warning(f'获取任务详情异常: {e}')
        return aiohttp_web.Response(text='获取任务详情异常', status=502, content_type='text/plain; charset=utf-8')

    if task_data.get('status') != 'done':
        return aiohttp_web.Response(text='任务尚未完成', status=409, content_type='text/plain; charset=utf-8')

    # 解析 result JSON 获取文件信息
    import json
    try:
        result_data = json.loads(task_data.get('result', '{}'))
    except (json.JSONDecodeError, TypeError):
        result_data = {}

    file_name = result_data.get('file_name', '')
    file_size = result_data.get('file_size', 0)
    file_title = result_data.get('title', '下载文件')

    if not file_name:
        return aiohttp_web.Response(text='文件信息缺失', status=404, content_type='text/plain; charset=utf-8')

    file_path = os.path.join(config.DOWNLOAD_DIR, file_name)

    if not os.path.exists(file_path):
        logger.warning(f'缓存文件不存在: {file_path}')
        return aiohttp_web.Response(text='缓存文件已过期，请重新下载', status=410, content_type='text/plain; charset=utf-8')

    # 确定 Content-Type
    ext = os.path.splitext(file_name)[1].lower()
    if ext == '.mp3':
        content_type = 'audio/mpeg'
    elif ext == '.mp4':
        content_type = 'video/mp4'
    else:
        content_type = 'application/octet-stream'

    actual_size = os.path.getsize(file_path)
    loop = asyncio.get_event_loop()

    # ── 解析 Range 请求头（支持断点续传）──
    range_header = request.headers.get('Range', '')
    if range_header:
        m = re.match(r'bytes=(\d+)-(\d*)', range_header)
        if m:
            start = int(m.group(1))
            end_str = m.group(2)
            end = int(end_str) if end_str else actual_size - 1

            if start >= actual_size:
                return aiohttp_web.Response(
                    status=416,
                    headers={'Content-Range': f'bytes */{actual_size}'},
                    text='Range Not Satisfiable'
                )

            end = min(end, actual_size - 1)
            content_length = end - start + 1

            resp = aiohttp_web.StreamResponse(
                status=206,
                headers={
                    'Content-Type': content_type,
                    'Content-Disposition': f'attachment; filename="{file_name}"',
                    'Content-Length': str(content_length),
                    'Content-Range': f'bytes {start}-{end}/{actual_size}',
                    'Accept-Ranges': 'bytes',
                }
            )
            await resp.prepare(request)

            _active_streams.add(file_path)
            try:
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    remaining = content_length
                    while remaining > 0:
                        chunk_size = min(65536, remaining)
                        chunk = await loop.run_in_executor(None, f.read, chunk_size)
                        if not chunk:
                            break
                        await resp.write(chunk)
                        remaining -= len(chunk)
                await resp.write_eof()
            finally:
                _active_streams.discard(file_path)
            return resp

    # ── 返回完整文件 ──
    resp = aiohttp_web.StreamResponse(
        headers={
            'Content-Type': content_type,
            'Content-Disposition': f'attachment; filename="{file_name}"',
            'Content-Length': str(actual_size),
            'Accept-Ranges': 'bytes',
        }
    )
    await resp.prepare(request)

    _active_streams.add(file_path)
    try:
        with open(file_path, 'rb') as f:
            chunk = await loop.run_in_executor(None, f.read, 65536)
            while chunk:
                await resp.write(chunk)
                chunk = await loop.run_in_executor(None, f.read, 65536)
        await resp.write_eof()
    finally:
        _active_streams.discard(file_path)
    return resp


async def _handle_download_http(request: aiohttp_web.Request):
    """下载 YouTube 视频并流式返回文件（支持缓存 + Range 断点续传）。

    流程：
    1. 检查缓存是否存在且未过期（<30 分钟）
    2. 缓存命中 → 跳过 yt-dlp，直接从缓存文件服务
    3. 缓存未命中 → 执行 yt-dlp，重命名文件以包含 format_id
    4. 解析 Range 请求头 → 206 Partial Content（断点续传）
    5. 文件不自动删除，由 _cleanup_temp_dir 定时清理（30 分钟阈值）
    """
    if not await _check_search_auth(request):
        logger.warning('下载请求认证失败（ticket 无效/过期或 Worker 不可达）')
        return aiohttp_web.Response(
            status=401,
            text='''<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>下载失败</title>
<style>
  body {{ font-family: -apple-system, "Noto Sans SC", sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: #fafafa; color: #333; }}
  .card {{ background: #fff; border-radius: 12px; padding: 2rem; max-width: 480px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); text-align: center; }}
  .icon {{ font-size: 3rem; margin-bottom: 0.5rem; }}
  h2 {{ margin: 0.5rem 0; font-size: 1.2rem; }}
  .err {{ color: #dc2626; font-size: 0.85rem; background: #fef2f2; padding: 0.75rem; border-radius: 8px; margin: 1rem 0; }}
</style></head>
<body><div class="card">
  <div class="icon">🔒</div>
  <h2>下载凭证无效或已过期</h2>
  <div class="err">请返回下载页面重新点击下载按钮获取新的凭证。</div>
</div></body></html>''',
            content_type='text/html; charset=utf-8'
        )

    # 优先使用 ticket 验证通过时携带的参数（防御深度：防止 URL 参数被篡改）
    if hasattr(request, '_ticket_data') and request._ticket_data:
        td = request._ticket_data
        url = td.get('url', '').strip()
        fmt = td.get('format', 'audio')
        fmt_id = td.get('format_id', '')
    else:
        url = request.rel_url.query.get('url', '').strip()
        fmt = request.rel_url.query.get('format', 'audio')
        fmt_id = request.rel_url.query.get('format_id', '')
    if not url:
        return aiohttp_web.json_response({'error': '缺少 url 参数'}, status=400)

    loop = asyncio.get_event_loop()
    file_path = None
    try:
        # ── 尝试使用缓存文件 ──
        video_id = _get_video_id(url)
        cache_path = None
        if video_id:
            if fmt == 'audio':
                cache_path = os.path.join(config.DOWNLOAD_DIR, f'{video_id}.mp3')
            elif fmt_id:
                cache_path = os.path.join(config.DOWNLOAD_DIR, f'{video_id}_{fmt_id}.mp4')

        now = time.time()
        if cache_path and os.path.exists(cache_path) and (now - os.path.getmtime(cache_path)) < 1800:
            file_path = cache_path
            logger.info(f'缓存命中: {os.path.basename(cache_path)}')
            content_type = 'video/mp4' if fmt == 'video' else 'audio/mpeg'

        # ── 缓存未命中，执行下载 ──
        if not file_path:
            # 主动检查磁盘空间，低于 2GB 时提前触发清理（保护其他服务不受影响）
            try:
                usage = shutil.disk_usage(config.DOWNLOAD_DIR)
                if usage.free < _MIN_FREE_SPACE:
                    logger.warning(f'⚠️ 磁盘剩余 {usage.free/1024/1024:.0f} MB，低于 2GB 警戒线，触发预防性清理...')
                    # 目标是释放到 2GB 以上，再多清 500MB 余量
                    target = _MIN_FREE_SPACE - usage.free + 500 * 1024**2
                    await _emergency_disk_cleanup(target_bytes=target)
            except Exception:
                pass  # 检查失败不阻塞下载

            try:
                if fmt == 'video' and fmt_id:
                    meta = await loop.run_in_executor(None, download_video, url, fmt_id)
                else:
                    meta = await loop.run_in_executor(None, download_audio, url)
            except OSError as e:
                if e.errno == 28 or 'No space left' in str(e):
                    logger.warning('⚠️ 磁盘空间仍然不足，二次紧急清理...')
                    # 估算需要释放的空间（当前下载文件大概需要 2 倍空间用于临时缓存 + 最终文件）
                    target = int(os.stat(file_path).st_size * 2) if file_path and os.path.exists(file_path) else 0
                    await _emergency_disk_cleanup(target_bytes=target)
                    # 清理后重试一次
                    if fmt == 'video' and fmt_id:
                        meta = await loop.run_in_executor(None, download_video, url, fmt_id)
                    else:
                        meta = await loop.run_in_executor(None, download_audio, url)
                else:
                    raise

            file_path = meta['file_path']
            content_type = meta.get('mime_type', 'application/octet-stream')

            # 视频重命名以包含 format_id，便于缓存区分不同分辨率
            if fmt == 'video' and fmt_id and cache_path and file_path != cache_path:
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                os.rename(file_path, cache_path)
                file_path = cache_path
                logger.info(f'缓存写入: {os.path.basename(cache_path)}')

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        # ── 解析 Range 请求头 ──
        range_header = request.headers.get('Range', '')
        if range_header:
            m = re.match(r'bytes=(\d+)-(\d*)', range_header)
            if m:
                start = int(m.group(1))
                end_str = m.group(2)
                end = int(end_str) if end_str else file_size - 1

                if start >= file_size:
                    return aiohttp_web.Response(
                        status=416,
                        headers={'Content-Range': f'bytes */{file_size}'},
                        text='Range Not Satisfiable'
                    )

                end = min(end, file_size - 1)
                content_length = end - start + 1

                resp = aiohttp_web.StreamResponse(
                    status=206,
                    headers={
                        'Content-Type': content_type,
                        'Content-Disposition': f'attachment; filename="{file_name}"',
                        'Content-Length': str(content_length),
                        'Content-Range': f'bytes {start}-{end}/{file_size}',
                        'Accept-Ranges': 'bytes',
                    }
                )
                await resp.prepare(request)

                _active_streams.add(file_path)
                try:
                    with open(file_path, 'rb') as f:
                        f.seek(start)
                        remaining = content_length
                        while remaining > 0:
                            chunk_size = min(65536, remaining)
                            chunk = await loop.run_in_executor(None, f.read, chunk_size)
                            if not chunk:
                                break
                            await resp.write(chunk)
                            remaining -= len(chunk)
                    await resp.write_eof()
                finally:
                    _active_streams.discard(file_path)
                return resp

        # ── 无 Range 请求头，返回完整文件 ──
        resp = aiohttp_web.StreamResponse(
            headers={
                'Content-Type': content_type,
                'Content-Disposition': f'attachment; filename="{file_name}"',
                'Content-Length': str(file_size),
                'Accept-Ranges': 'bytes',
            }
        )
        await resp.prepare(request)

        _active_streams.add(file_path)
        try:
            with open(file_path, 'rb') as f:
                chunk = await loop.run_in_executor(None, f.read, 65536)
                while chunk:
                    await resp.write(chunk)
                    chunk = await loop.run_in_executor(None, f.read, 65536)
            await resp.write_eof()
        finally:
            _active_streams.discard(file_path)
        return resp

    except Exception as e:
        logger.exception(f'下载处理失败: {url}')
        import html as _html
        err_msg = _html.escape(str(e)[:500])
        # 返回 HTML 错误页而非 JSON，让用户在新标签中看到具体的错误信息
        html_error = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>下载失败</title>
<style>
  body {{ font-family: -apple-system, "Noto Sans SC", sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: #fafafa; color: #333; }}
  .card {{ background: #fff; border-radius: 12px; padding: 2rem; max-width: 480px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); text-align: center; }}
  .icon {{ font-size: 3rem; margin-bottom: 0.5rem; }}
  h2 {{ margin: 0.5rem 0; font-size: 1.2rem; }}
  .err {{ color: #dc2626; font-size: 0.88rem; background: #fef2f2; padding: 0.75rem; border-radius: 8px; margin: 1rem 0; word-break: break-word; text-align: left; }}
  .hint {{ font-size: 0.82rem; color: #6b7280; }}
  .btn {{ display: inline-block; margin-top: 1rem; padding: 0.5rem 1.2rem; background: #1a3a2a; color: #fff; border-radius: 6px; text-decoration: none; font-size: 0.88rem; }}
</style></head>
<body><div class="card">
  <div class="icon">❌</div>
  <h2>下载处理失败</h2>
  <div class="err">{err_msg}</div>
  <p class="hint">请检查 YouTube 链接是否有效，或稍后重试。</p>
  <a class="btn" href="javascript:window.close()">关闭此页</a>
</div></body></html>'''
        return aiohttp_web.Response(text=html_error, status=500, content_type='text/html; charset=utf-8')
    # 文件不自动删除，由 _cleanup_temp_dir 定时清理（.mp3/.mp4 超过 30 分钟自动删除）


async def _emergency_disk_cleanup(target_bytes: int = 0, exclude_path: str = None) -> int:
    """磁盘满时紧急清理：删除最旧的未在下载中的已完成缓存文件。

    Args:
        target_bytes: 需要释放的目标字节数（0 = 全部可删文件都删光）
        exclude_path: 排除的文件路径（不删除自己）
    """
    if not os.path.isdir(config.DOWNLOAD_DIR):
        return 0

    candidates = []
    exclude_abs = os.path.abspath(exclude_path) if exclude_path else None

    for fname in os.listdir(config.DOWNLOAD_DIR):
        fpath = os.path.join(config.DOWNLOAD_DIR, fname)
        if not os.path.isfile(fpath):
            continue
        # 只清理已完成文件（非 .part 等临时文件）
        if not fname.endswith(('.mp3', '.mp4', '.m4a', '.webm')):
            continue
        # 跳过正在被用户下载的文件
        if fpath in _active_streams:
            continue
        # 跳过自己
        if exclude_abs and os.path.abspath(fpath) == exclude_abs:
            continue
        candidates.append((os.path.getmtime(fpath), fpath))

    candidates.sort()  # 最旧的在前（优先删除最早缓存的文件）
    deleted = 0
    freed = 0
    for _, fpath in candidates:
        try:
            freed += os.path.getsize(fpath)
            os.remove(fpath)
            deleted += 1
            logger.info(f'🧹 磁盘满紧急清理: {os.path.basename(fpath)}')
            if target_bytes and freed >= target_bytes:
                break
        except Exception as e:
            logger.warning(f'紧急清理删除失败: {fpath}: {e}')

    if deleted:
        logger.info(f'🧹 紧急清理完成：删除了 {deleted} 个文件，释放 {freed/1024/1024:.0f} MB')
    return deleted


async def _start_search_server():
    """启动 HTTP 搜索/下载服务（端口 8080，仅 bot0）。
    禁用 aiohttp 默认 access log，避免被互联网扫描请求刷屏。
    """
    try:
        # 禁用默认 access log（扫描器会疯狂请求 / /login /sitemap.xml 等路径）
        logging.getLogger('aiohttp.access').setLevel(logging.WARNING)

        app = aiohttp_web.Application()
        app.router.add_get('/search', _handle_search_http)
        app.router.add_get('/download', _handle_download_http)
        app.router.add_get('/download-file', _handle_download_file_http)
        app.router.add_get('/formats', _handle_formats_http)

        # catch-all 路由：未知路径立即返回空响应，避免不必要的事件循环开销
        async def _catch_all(_request):
            return aiohttp_web.Response(status=404)
        app.router.add_route('*', '/{tail:.*}', _catch_all)

        runner = aiohttp_web.AppRunner(app, access_log=None)
        await runner.setup()
        site = aiohttp_web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        logger.info('🔍 搜索+下载服务已启动，端口 8080')
    except Exception as e:
        logger.error(f'🔍 搜索+下载服务启动失败: {e}')

# ──────────────────启动──────────────────

async def _auto_refresh_jwt():
    """定时刷新 JWT。失败时先 1s/2s/4s 快速重试 3 次，之后每 5 分钟重试一次。"""
    startup_retries = [1, 2, 4]  # 启动时快速重试间隔（秒）
    attempt = 0
    while True:
        try:
            jwt = await refresh_jwt()
            if jwt:
                logger.info('✅ JWT 已刷新')
                attempt = 0
                await asyncio.sleep(23 * 3600)
                continue
        except Exception as e:
            logger.warning(f'⚠️ JWT 刷新异常 [{type(e).__name__}]: {e}')
        # 失败：按尝试次数决定等待时长
        if attempt < len(startup_retries):
            delay = startup_retries[attempt]
            attempt += 1
            logger.info(f'⏳ JWT 刷新失败，{delay}s 后快速重试（第 {attempt}/3 次）')
        else:
            delay = 300  # 5 分钟
            logger.info(f'⏳ JWT 刷新失败，{delay}s 后重试')
        await asyncio.sleep(delay)

async def _cleanup_temp_dir():
    """定时清理下载中断残留的临时文件。"""
    while True:
        try:
            if not os.path.isdir(config.DOWNLOAD_DIR):
                await asyncio.sleep(1800)
                continue
            now = time.time()
            cleaned = 0
            for fname in os.listdir(config.DOWNLOAD_DIR):
                fpath = os.path.join(config.DOWNLOAD_DIR, fname)
                if not os.path.isfile(fpath):
                    continue
                age = now - os.path.getmtime(fpath)
                # .part 或 .ytdl 等临时后缀 → 超过 30 分钟即删除（跳过正在传输的）
                if fname.endswith(('.part', '.ytdl', '.fragment', '.temp')) and age > 1800 and fpath not in _active_streams:
                    os.remove(fpath)
                    cleaned += 1
                # 已完成但未清理的 .mp3/.mp4 → 超过 30 分钟删除（跳过正在传输的）
                elif fname.endswith(('.mp3', '.mp4', '.m4a', '.webm')) and age > 1800 and fpath not in _active_streams:
                    os.remove(fpath)
                    cleaned += 1
            if cleaned:
                logger.info(f'🧹 已清理 {cleaned} 个残留文件（{config.DOWNLOAD_DIR}）')
        except Exception as e:
            logger.warning(f'临时文件清理异常: {e}')
        await asyncio.sleep(1800)  # 每 30 分钟执行一次

async def _alert_admin(msg: str):
    """向所有管理员发送 Telegram 通知。不会阻塞，fire-and-forget。"""
    if not _bot_app:
        return
    for uid in config.ADMIN_IDS:
        try:
            await _bot_app.bot.send_message(chat_id=uid, text=msg, disable_notification=False)
        except Exception as e:
            logger.warning(f'发送管理员通知失败 (uid={uid}): {e}')

async def post_init(app):
    global _bot_app
    _bot_app = app
    asyncio.create_task(_auto_refresh_jwt())
    asyncio.create_task(_task_poller())
    asyncio.create_task(_cleanup_temp_dir())
    if config.BOT_ID == 'bot0':
        asyncio.create_task(_start_search_server())
    await app.bot.set_my_commands([
        BotCommand('start',    '查看帮助'),
        BotCommand('search',   '搜索 YouTube 视频'),
        BotCommand('auto',     '自动下载第一个结果（音频）'),
        BotCommand('add',      '直接上传指定链接'),
        BotCommand('playlist', '下载整个播放列表'),
        BotCommand('cancel',  '取消正在进行的下载'),
        BotCommand('channel', '下载整个频道（音频）'),
        BotCommand('category', '指定分类上传'),
    ])
    logger.info(f'🎵 赞美诗 Bot 已启动（{config.BOT_ID}）')

def main():
    app = (
        Application.builder()
        .token(config.BOT_TOKEN)
        .base_url(f'{config.TG_API_BASE}/bot')
        .base_file_url(f'{config.TG_API_BASE}/file/bot')
        .local_mode(True)
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler('start',    cmd_start))
    app.add_handler(CommandHandler('help',     cmd_start))
    app.add_handler(CommandHandler('search',   cmd_search))
    app.add_handler(CommandHandler('auto',     cmd_auto))
    app.add_handler(CommandHandler('add',      cmd_add))
    app.add_handler(CommandHandler('category', cmd_category))
    app.add_handler(CommandHandler('playlist', cmd_playlist))
    app.add_handler(CommandHandler('channel', cmd_channel))
    app.add_handler(CommandHandler('cancel',  cmd_cancel))
    app.add_handler(CallbackQueryHandler(callback_pick,     pattern=r'^pick:'))
    app.add_handler(CallbackQueryHandler(callback_format,   pattern=r'^fmt:'))
    app.add_handler(CallbackQueryHandler(callback_playlist, pattern=r'^pl:'))
    app.add_handler(CallbackQueryHandler(callback_retry_playlist_failed, pattern=r'^rpl:'))
    app.add_handler(CallbackQueryHandler(callback_channel, pattern=r'^ch:'))
    app.add_handler(CallbackQueryHandler(callback_retry_playlist_failed, pattern=r'^rch:'))
    app.add_handler(CallbackQueryHandler(callback_channel, pattern=r'^chpl:'))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
