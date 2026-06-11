#!/usr/bin/env python3
"""
SHA-256 历史数据回写迁移脚本

针对已存在的 hymns/files 记录（sha256 为 NULL），
通过本地 Telegram Bot API 容器下载文件，计算 SHA-256 并写回 D1 数据库。

使用方式：
    python migrate_sha256.py [--dry-run] [--limit N] [--concurrency N]

环境要求：
    .env 文件（或环境变量）包含：BOT_TOKEN, CF_WORKER_URL, CF_API_KEY
    TG_API_BASE 默认为 http://telegram-bot-api:8081
"""

import asyncio
import argparse
import hashlib
import logging
import os
import json
import sys

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

TG_API_BASE   = os.getenv('TG_API_BASE', 'http://telegram-bot-api:8081')
BOT_TOKEN     = os.getenv('BOT_TOKEN', '')
CF_WORKER_URL = os.getenv('CF_WORKER_URL', '').rstrip('/')
CF_API_KEY    = os.getenv('CF_API_KEY', '')
CF_JWT        = ''

_BOT_POOL: list[dict] = []
try:
    _pool_raw = os.getenv('BOT_POOL', '')
    if _pool_raw:
        _BOT_POOL = json.loads(_pool_raw)
except Exception:
    pass


def _get_bot_token(bot_index: int) -> str:
    if _BOT_POOL and 0 <= bot_index < len(_BOT_POOL):
        return _BOT_POOL[bot_index]['token']
    return BOT_TOKEN


def _admin_headers() -> dict:
    return {
        'X-Admin-Token': CF_API_KEY,
        'Authorization': f'Bearer {CF_JWT}',
    }


async def _refresh_jwt(session: aiohttp.ClientSession):
    global CF_JWT
    try:
        async with session.post(
            f'{CF_WORKER_URL}/api/admin/login',
            json={'token': CF_API_KEY},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            data = await resp.json()
            CF_JWT = data.get('sessionToken', '')
            if CF_JWT:
                logger.info('JWT 已刷新')
    except Exception as e:
        logger.warning(f'JWT 刷新失败: {e}')


async def _get_tg_download_url(session: aiohttp.ClientSession, file_id: str, bot_token: str) -> str | None:
    url = f'{TG_API_BASE}/bot{bot_token}/getFile'
    try:
        async with session.get(url, params={'file_id': file_id},
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            data = await resp.json()
            if not data.get('ok'):
                logger.warning(f'  getFile 失败: {data.get("description")} (file_id={file_id[:20]}...)')
                return None
            return f'{TG_API_BASE}/file/bot{bot_token}/{data["result"]["file_path"]}'
    except Exception as e:
        logger.warning(f'  getFile 异常: {e}')
        return None


async def _stream_sha256(session: aiohttp.ClientSession, url: str) -> str | None:
    """流式下载并计算 SHA-256，不将整个文件载入内存"""
    h = hashlib.sha256()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as resp:
            if resp.status != 200:
                logger.warning(f'  下载失败 HTTP {resp.status}')
                return None
            async for chunk in resp.content.iter_chunked(65536):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.warning(f'  下载异常: {e}')
        return None


async def _patch_sha256(
    session: aiohttp.ClientSession,
    table: str, record_id: int, sha256: str,
    dry_run: bool
) -> bool:
    if dry_run:
        logger.info(f'  [dry-run] {table}#{record_id} sha256={sha256[:16]}...')
        return True
    url = f'{CF_WORKER_URL}/api/admin/sha256-patch'
    payload = {'table': table, 'id': record_id, 'sha256': sha256}
    headers = {**_admin_headers(), 'Content-Type': 'application/json'}
    try:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 401:
                await _refresh_jwt(session)
                async with session.post(url, json=payload,
                                        headers={**_admin_headers(), 'Content-Type': 'application/json'},
                                        timeout=aiohttp.ClientTimeout(total=15)) as resp2:
                    return resp2.status == 200
            return resp.status == 200
    except Exception as e:
        logger.warning(f'  写回失败: {e}')
        return False


async def _fetch_null_records(session: aiohttp.ClientSession, limit: int) -> list[dict]:
    url = f'{CF_WORKER_URL}/api/admin/sha256-null-records'
    try:
        async with session.get(
            url, params={'limit': str(limit)}, headers=_admin_headers(),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status == 401:
                await _refresh_jwt(session)
                async with session.get(
                    url, params={'limit': str(limit)}, headers=_admin_headers(),
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp2:
                    return (await resp2.json()).get('records', [])
            return (await resp.json()).get('records', [])
    except Exception as e:
        logger.error(f'获取记录失败: {e}')
        return []


async def _process_record(
    session: aiohttp.ClientSession,
    rec: dict,
    dry_run: bool,
    sem: asyncio.Semaphore,
    stats: dict
):
    async with sem:
        table     = rec['table']
        rec_id    = rec['id']
        file_name = rec.get('file_name', '')
        is_multi  = False

        try:
            parts = json.loads(rec.get('file_parts', '[]') or '[]')
        except Exception:
            logger.warning(f'[{table}#{rec_id}] file_parts 解析失败，跳过')
            stats['skip'] += 1
            return

        if not parts:
            logger.warning(f'[{table}#{rec_id}] file_parts 为空，跳过')
            stats['skip'] += 1
            return

        is_multi = len(parts) > 1
        logger.info(f'[{table}#{rec_id}] {file_name} ({len(parts)} 分片)')

        # 多分片：下载所有分片拼接后计算，与前端上传 SHA-256 保持一致
        h = hashlib.sha256()
        ok_download = True
        for i, part in enumerate(parts):
            if isinstance(part, str):
                tg_file_id, bot_index = part, 0
            else:
                tg_file_id = part.get('id', '')
                bot_index  = part.get('b', 0)

            if not tg_file_id:
                logger.warning(f'  分片 {i} 无 file_id，跳过整条')
                ok_download = False
                break

            bot_token = _get_bot_token(bot_index)
            dl_url = await _get_tg_download_url(session, tg_file_id, bot_token)
            if not dl_url:
                ok_download = False
                break

            # 流式下载更新同一个 hasher
            try:
                async with session.get(dl_url, timeout=aiohttp.ClientTimeout(total=600)) as resp:
                    if resp.status != 200:
                        logger.warning(f'  分片 {i} 下载失败 HTTP {resp.status}')
                        ok_download = False
                        break
                    async for chunk in resp.content.iter_chunked(65536):
                        h.update(chunk)
            except Exception as e:
                logger.warning(f'  分片 {i} 下载异常: {e}')
                ok_download = False
                break

        if not ok_download:
            stats['fail'] += 1
            return

        sha256 = h.hexdigest()
        success = await _patch_sha256(session, table, rec_id, sha256, dry_run)
        if success:
            label = '(dry-run)' if dry_run else ''
            logger.info(f'  ✅ sha256={sha256[:16]}... {label}')
            stats['ok'] += 1
        else:
            logger.warning(f'  ❌ 写回失败')
            stats['fail'] += 1


async def main(dry_run: bool, limit: int, concurrency: int):
    if not CF_WORKER_URL or not CF_API_KEY:
        logger.error('缺少 CF_WORKER_URL 或 CF_API_KEY，请检查 .env')
        sys.exit(1)
    if not BOT_TOKEN and not _BOT_POOL:
        logger.error('缺少 BOT_TOKEN 或 BOT_POOL，请检查 .env')
        sys.exit(1)

    connector = aiohttp.TCPConnector(limit=concurrency + 8)
    async with aiohttp.ClientSession(connector=connector) as session:
        await _refresh_jwt(session)

        logger.info(f'查询 sha256=NULL 记录（limit={limit}）...')
        records = await _fetch_null_records(session, limit)
        total = len(records)
        logger.info(f'共 {total} 条需要处理')
        if not records:
            logger.info('无需迁移，已完成。')
            return

        stats = {'ok': 0, 'fail': 0, 'skip': 0}
        sem = asyncio.Semaphore(concurrency)
        tasks = [
            _process_record(session, rec, dry_run, sem, stats)
            for rec in records
        ]
        await asyncio.gather(*tasks)

    logger.info(
        f'迁移完成！成功={stats["ok"]} 失败={stats["fail"]} 跳过={stats["skip"]} / 共={total}'
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SHA-256 历史数据迁移脚本')
    parser.add_argument('--dry-run',     action='store_true', help='只打印，不实际写入数据库')
    parser.add_argument('--limit',       type=int, default=500, help='最多处理多少条记录（默认 500）')
    parser.add_argument('--concurrency', type=int, default=3,   help='并发数（默认 3，避免 TG 频率限制）')
    args = parser.parse_args()

    if args.dry_run:
        logger.info('=== DRY-RUN 模式，不会写入数据库 ===')

    asyncio.run(main(args.dry_run, args.limit, args.concurrency))
