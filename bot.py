import asyncio
import logging
import os
import aiohttp
from aiohttp import web as aiohttp_web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)
from config import config
from downloader import search_youtube, get_formats, get_playlist_info, download_audio, download_video
from uploader import direct_upload, refresh_jwt

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

search_cache:   dict = {}   # uid -> [results]
format_cache:   dict = {}   # uid -> {url, formats_info}
playlist_cache: dict = {}   # uid -> {entries, title, count}

def is_admin(user_id: int) -> bool:
    return not config.ADMIN_IDS or user_id in config.ADMIN_IDS

def fmt_dur(seconds) -> str:
    s = int(seconds or 0)
    return f"{s//60}:{s%60:02d}"

# ──────────────────命令──────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "🎵 *赞美诗资源机器人*\n\n"
        "*/search* `关键词` — 搜索并列出候选\n"
        "*/auto* `关键词` — 自动下载第一个（音频）\n"
        "*/add* `URL` — 直接上传指定链接\n"
        "*/playlist* `URL` — 下载整个播放列表\n"
        "*/category* `关键词` `分类` — 指定分类上传\n\n"
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
        search_cache[update.effective_user.id] = results
        lines, buttons = [], []
        for r in results:
            dur = fmt_dur(r.get('duration'))
            lines.append(f"`{r['index']}`. {r['title']} [{dur}]\n   _{r['uploader']}_")
            buttons.append([InlineKeyboardButton(
                f"⬇️ {r['index']}. {r['title'][:35]}",
                callback_data=f"pick:{update.effective_user.id}:{r['index']-1}"
            )])
        await msg.edit_text(
            f"🎵 *{keyword}* 结果：\n\n" + '\n\n'.join(lines),
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

    playlist_cache[uid] = info
    total_dur = fmt_dur(info['total_duration'])

    buttons = [
        [InlineKeyboardButton('🎵 全部音频 MP3', callback_data=f'pl:{uid}:audio:0')],
        [InlineKeyboardButton('🎬 全部视频 360p', callback_data=f'pl:{uid}:video:360')],
        [InlineKeyboardButton('🎬 全部视频 480p', callback_data=f'pl:{uid}:video:480')],
        [InlineKeyboardButton('🎬 全部视频 720p', callback_data=f'pl:{uid}:video:720')],
        [InlineKeyboardButton('🎬 全部视频 1080p', callback_data=f'pl:{uid}:video:1080')],
    ]
    await msg.edit_text(
        f"📋 *{info['title']}*\n"
        f"🎵 共 {info['count']} 个视频\n"
        f"⏱ 总时长：{total_dur}\n\n"
        f"请选择下载格式：",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='Markdown'
    )

# ──────────────────格式选择──────────────────

async def _show_format_picker(msg, url: str, uid: int):
    await msg.reply_text(f'🔍 获取格式信息...')
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, get_formats, url)
    except Exception as e:
        await msg.reply_text(f'❌ 无法获取格式：{e}'); return

    format_cache[uid] = {'url': url, 'info': info}

    buttons = [
        [InlineKeyboardButton('🎵 音频 MP3', callback_data=f'fmt:{uid}:audio:0')],
    ]
    for vf in info['video_formats']:
        buttons.append([InlineKeyboardButton(
            f'🎬 视频 {vf["height"]}p',
            callback_data=f'fmt:{uid}:video:{vf["format_id"]}'
        )])

    await msg.reply_text(
        f"🎵 *{info['title']}*\n⏱ {fmt_dur(info['duration'])}\n\n请选择下载格式：",
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

    format_cache[uid] = {'url': item['url'], 'info': info}

    buttons = [
        [InlineKeyboardButton('🎵 音频 MP3', callback_data=f'fmt:{uid}:audio:0')],
    ]
    for vf in info['video_formats']:
        buttons.append([InlineKeyboardButton(
            f'🎬 视频 {vf["height"]}p',
            callback_data=f'fmt:{uid}:video:{vf["format_id"]}'
        )])

    await query.edit_message_text(
        f"🎵 *{info['title']}*\n⏱ {fmt_dur(info['duration'])}\n\n请选择下载格式：",
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

    await query.edit_message_text(
        f"⬇️ 下载中：{'音频 MP3' if fmt == 'audio' else f'视频 {fmt_id}p'}\n🎵 {info['title']}..."
    )
    await _do_download_and_upload(query.message, url, {}, fmt, fmt_id if fmt == 'video' else None)

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

    await query.edit_message_text(
        f"⬇️ 开始下载播放列表：*{info['title']}*\n"
        f"共 {total} 个，格式：{'音频 MP3' if fmt == 'audio' else f'视频 {res}p'}\n"
        f"进度：0/{total}",
        parse_mode='Markdown'
    )

    success, failed = 0, 0
    loop = asyncio.get_event_loop()

    for i, entry in enumerate(entries, 1):
        try:
            await query.edit_message_text(
                f"⬇️ *{info['title']}*\n"
                f"进度：{i}/{total} — {entry['title'][:40]}",
                parse_mode='Markdown'
            )
            if fmt == 'audio':
                meta = await loop.run_in_executor(None, download_audio, entry['url'])
            else:
                fmts = await loop.run_in_executor(None, get_formats, entry['url'])
                target_h = int(res)
                matched = next((f for f in fmts['video_formats'] if f['height'] == target_h), None)
                if not matched:
                    matched = min(fmts['video_formats'], key=lambda f: abs(f['height'] - target_h), default=None)
                if not matched:
                    failed += 1
                    continue
                meta = await loop.run_in_executor(None, download_video, entry['url'], matched['format_id'])

            result = await direct_upload(meta['file_path'], meta)
            try:
                os.remove(meta['file_path'])
            except Exception:
                pass
            success += 1
        except Exception as e:
            logger.error(f'播放列表第{i}项失败：{e}')
            failed += 1

    await query.edit_message_text(
        f"✅ *播放列表下载完成*\n\n"
        f"📋 {info['title']}\n"
        f"✅ 成功：{success}\n"
        f"❌ 失败：{failed}",
        parse_mode='Markdown'
    )

# ──────────────────核心逻辑──────────────────

async def _do_download_and_upload(msg, url: str, metadata: dict, fmt: str, fmt_id):
    loop = asyncio.get_event_loop()
    try:
        if fmt == 'video':
            await msg.edit_text('⬇️ 正在下载视频...')
            meta = await loop.run_in_executor(None, download_video, url, fmt_id)
        else:
            await msg.edit_text('⬇️ 正在下载音频...')
            meta = await loop.run_in_executor(None, download_audio, url)

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
            f"🎵 {meta['title']}\n"
            f"📂 {meta.get('category', config.DEFAULT_CATEGORY)}\n"
            f"⏱ {fmt_dur(meta.get('duration'))}\n"
            f"🆔 ID：`{result.get('id', '?')}`",
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

async def _patch_task(session: aiohttp.ClientSession, task_id: int, **kwargs):
    url = f"{config.CF_WORKER_URL}/api/bot/tasks/{task_id}"
    try:
        async with session.patch(url, json=kwargs, headers=_worker_headers()) as r:
            if r.status != 200:
                logger.warning(f'[{config.BOT_ID}] patch task {task_id} failed: {r.status}')
    except Exception as e:
        logger.warning(f'[{config.BOT_ID}] patch task {task_id} error: {e}')

async def _execute_task(session: aiohttp.ClientSession, task: dict):
    task_id = task['id']
    url = task['url']
    mode = task.get('mode', 'audio')
    fmt = task.get('format')
    category = task.get('category') or config.DEFAULT_CATEGORY

    logger.info(f'[{config.BOT_ID}] 开始任务 #{task_id}: {mode} {url}')
    loop = asyncio.get_event_loop()

    try:
        await _patch_task(session, task_id, status='processing', progress='下载中...')
        if mode == 'video':
            meta = await loop.run_in_executor(None, download_video, url, fmt)
        else:
            meta = await loop.run_in_executor(None, download_audio, url)

        meta['category'] = category
        size_mb = os.path.getsize(meta['file_path']) / 1024 / 1024
        await _patch_task(session, task_id, progress=f'上传中（{size_mb:.1f} MB）...')

        result = await direct_upload(meta['file_path'], meta)

        try:
            os.remove(meta['file_path'])
        except Exception:
            pass

        import json
        await _patch_task(
            session, task_id,
            status='done',
            progress='完成',
            result=json.dumps({'id': result.get('id'), 'title': meta.get('title', '')}, ensure_ascii=False)
        )
        logger.info(f'[{config.BOT_ID}] 任务 #{task_id} 完成，hymn_id={result.get("id")}')

    except Exception as e:
        logger.exception(f'[{config.BOT_ID}] 任务 #{task_id} 失败')
        try:
            os.remove(meta['file_path'])
        except Exception:
            pass
        await _patch_task(session, task_id, status='failed', error=str(e)[:500])

async def _task_poller():
    if not config.CF_WORKER_URL or not config.CF_API_KEY:
        logger.warning(f'[{config.BOT_ID}] CF_WORKER_URL 或 CF_API_KEY 未配置，任务轮询已禁用')
        return

    logger.info(f'[{config.BOT_ID}] 任务轮询已启动，间隔 {config.POLL_INTERVAL}s')
    poll_url = f"{config.CF_WORKER_URL}/api/bot/tasks/poll"

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(
                    poll_url,
                    json={'bot_id': config.BOT_ID},
                    headers=_worker_headers()
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        task = data.get('task')
                        if task:
                            asyncio.create_task(_execute_task(session, task))
                        await asyncio.sleep(0.5 if task else config.POLL_INTERVAL)
                    else:
                        logger.warning(f'[{config.BOT_ID}] poll 失败: {resp.status}')
                        await asyncio.sleep(config.POLL_INTERVAL)
            except Exception as e:
                logger.warning(f'[{config.BOT_ID}] 轮询异常: {e}')
                await asyncio.sleep(config.POLL_INTERVAL)

# ──────────────────HTTP 搜索服务（8080，仅 bot0）──────────────────

async def _verify_jwt_with_backend(token: str) -> bool:
    """转发 token 到后端 /api/admin/me 验证是否有效"""
    if not config.CF_WORKER_URL or not token:
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{config.CF_WORKER_URL}/api/admin/me",
                headers={'Authorization': f'Bearer {token}'},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                return resp.status == 200
    except Exception as e:
        logger.warning(f'验证 JWT 异常: {e}')
        return False

async def _check_search_auth(request: aiohttp_web.Request) -> bool:
    """验证请求身份：支持 CF_API_KEY（内部）或 JWT Bearer Token（前端用户）"""
    if request.headers.get('X-Admin-Token', '') == config.CF_API_KEY:
        return True
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        token = auth[7:].strip()
        return await _verify_jwt_with_backend(token)
    return False

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

async def _start_search_server():
    try:
        app = aiohttp_web.Application()
        app.router.add_get('/search', _handle_search_http)
        runner = aiohttp_web.AppRunner(app)
        await runner.setup()
        site = aiohttp_web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        logger.info('🔍 搜索服务已启动，端口 8080')
    except Exception as e:
        logger.error(f'🔍 搜索服务启动失败: {e}')

# ──────────────────启动──────────────────

async def _auto_refresh_jwt():
    while True:
        try:
            jwt = await refresh_jwt()
            logger.info('✅ JWT 已刷新' if jwt else '⚠️ JWT 刷新失败')
        except Exception as e:
            logger.error(f'JWT 刷新异常：{e}')
        await asyncio.sleep(23 * 3600)

async def post_init(app):
    asyncio.create_task(_auto_refresh_jwt())
    asyncio.create_task(_task_poller())
    if config.BOT_ID == 'bot0':
        asyncio.create_task(_start_search_server())
    # 注册指令菜单（覆盖式，重复执行无副作用）
    await app.bot.set_my_commands([
        BotCommand('start',    '查看帮助'),
        BotCommand('search',   '搜索 YouTube 视频'),
        BotCommand('auto',     '自动下载第一个结果（音频）'),
        BotCommand('add',      '直接上传指定链接'),
        BotCommand('playlist', '下载整个播放列表'),
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
    app.add_handler(CallbackQueryHandler(callback_pick,     pattern=r'^pick:'))
    app.add_handler(CallbackQueryHandler(callback_format,   pattern=r'^fmt:'))
    app.add_handler(CallbackQueryHandler(callback_playlist, pattern=r'^pl:'))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
