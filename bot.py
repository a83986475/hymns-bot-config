import asyncio
import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)
from config import config
from downloader import search_youtube, get_formats, download_audio, download_video
from uploader import direct_upload, refresh_jwt

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

search_cache: dict = {}   # uid -> [results]
format_cache: dict = {}   # uid -> {url, formats_info}

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
        "*/category* `关键词` `分类` — 指定分类上传\n\n"
        "分类：`诗歌音频` `歌谱乐谱` `歌词文本` `教程资料`",
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
    logger.info('🎵 赞美诗 Bot 已启动（直连模式）')

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
    app.add_handler(CallbackQueryHandler(callback_pick,   pattern=r'^pick:'))
    app.add_handler(CallbackQueryHandler(callback_format, pattern=r'^fmt:'))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
