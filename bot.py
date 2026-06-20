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
from config import config
from downloader import search_youtube, get_formats, get_playlist_info, download_audio, download_video, SUPPORTED_HEIGHTS, HEIGHT_LABELS
from uploader import direct_upload, refresh_jwt

# 每 bot 最多 1 个并发下载+上传任务，防止 Telegram flood control
_task_semaphore = asyncio.Semaphore(1)

# 追踪当前正在通过 HTTP 流式传输的文件（磁盘满时紧急清理用）
_active_streams: set = set()

# 磁盘空间警戒线：剩余空间低于此值时触发预防性清理（保护 VPS 上其他服务）
_MIN_FREE_SPACE = 2 * 1024**3  # 2GB

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

def _height_label(h: int) -> str:
    return HEIGHT_LABELS.get(h, f'{h}p')

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
    ]
    for h in sorted(SUPPORTED_HEIGHTS):
        buttons.append([InlineKeyboardButton(
            f'🎬 全部视频 {_height_label(h)}',
            callback_data=f'pl:{uid}:video:{h}'
        )])

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
            f'🎬 视频 {_height_label(vf["height"])}',
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
            f'🎬 视频 {_height_label(vf["height"])}',
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
                        f"🔄 重试 {i}/{total} — {entry['title'][:35]}...\n📋 {info['title']}",
                        parse_mode='Markdown'
                    )
                    await asyncio.sleep(2)

                await query.edit_message_text(
                    f"⬇️ {'重试' if attempt > 0 else '下载'}中 {i}/{total} — {entry['title'][:40]}\n📋 {info['title']}",
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

    await query.edit_message_text(
        f"⬇️ 开始下载播放列表：*{info['title']}*\n"
        f"共 {total} 个，格式：{'音频 MP3' if fmt == 'audio' else f'视频 {res}p'}\n"
        f"进度：0/{total}",
        parse_mode='Markdown'
    )

    success, failed, failed_items = await _process_playlist_entries(
        query, uid, info, entries, total, fmt, res
    )

    # 构建最终消息
    result_msg = f"{'✅' if failed == 0 else '⚠️'} *播放列表下载完成*\n\n"
    result_msg += f"📋 {info['title']}\n"
    result_msg += f"✅ 成功：{success}\n"
    if failed > 0:
        result_msg += f"❌ 失败：{failed}\n"
        # 列出失败项标题（最多显示 5 个，避免消息过长）
        for item in failed_items[:5]:
            result_msg += f"   • {item['title'][:40]}\n"
        if len(failed_items) > 5:
            result_msg += f'   … 还有 {len(failed_items) - 5} 项\n'

    buttons = []
    if failed > 0:
        # 保存失败项到缓存，供重试按钮使用
        if 'failed_items' not in playlist_cache:
            playlist_cache['failed_items'] = {}
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

    await query.edit_message_text(
        f"🔄 开始重试 {total} 个失败项...\n"
        f"格式：{'音频 MP3' if fmt == 'audio' else f'视频 {res}p'}\n"
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
            result_msg += f"   • {item['title'][:40]}\n"
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

                # 每项之间延迟 2-5 秒，避免触发频率限制
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

        # ── 非播放列表：单个文件下载 ──
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
        logger.exception(f'[{config.BOT_ID}] 任务 #{task_id} 失败')
        try:
            os.remove(meta['file_path'])
        except Exception:
            pass
        await _patch_task_retry(session, task_id, status='failed', error=str(e)[:500])

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

async def _check_search_auth(request: aiohttp_web.Request) -> bool:
    if request.headers.get('X-Admin-Token', '') == config.CF_API_KEY:
        return True
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        token = auth[7:].strip()
        return await _verify_jwt_with_backend(token)
    # 支持从 query parameter 传递 JWT（用于新标签页直连下载）
    query_token = request.rel_url.query.get('token', '')
    if query_token:
        return await _verify_jwt_with_backend(query_token)
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
        return aiohttp_web.Response(status=401, text='Unauthorized')

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
        return aiohttp_web.json_response({'error': str(e)[:500]}, status=500)
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
    try:
        app = aiohttp_web.Application()
        app.router.add_get('/search', _handle_search_http)
        app.router.add_get('/download', _handle_download_http)
        app.router.add_get('/formats', _handle_formats_http)
        runner = aiohttp_web.AppRunner(app)
        await runner.setup()
        site = aiohttp_web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        logger.info('🔍 搜索+下载服务已启动，端口 8080')
    except Exception as e:
        logger.error(f'🔍 搜索+下载服务启动失败: {e}')

# ──────────────────启动──────────────────

async def _auto_refresh_jwt():
    while True:
        try:
            jwt = await refresh_jwt()
            logger.info('✅ JWT 已刷新' if jwt else '⚠️ JWT 刷新失败')
        except Exception as e:
            logger.error(f'JWT 刷新异常：{e}')
        await asyncio.sleep(23 * 3600)

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
                # .part 或 .ytdl 等临时后缀 → 超过 10 分钟即删除
                if fname.endswith(('.part', '.ytdl', '.fragment', '.temp')) and age > 600:
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

async def post_init(app):
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
    app.add_handler(CallbackQueryHandler(callback_retry_playlist_failed, pattern=r'^rpl:'))
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
