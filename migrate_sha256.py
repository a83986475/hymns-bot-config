#!/usr/bin/env python3
"""
SHA-256 历史数据回写迁移脚本

下载策略：
  1. 先用本地 TG Bot API 容器（http://telegram-bot-api:8081）
  2. 本地容器返回 404 或失败，自动回退到公共 https://api.telegram.org
"""

import asyncio
import argparse
import hashlib
import logging
import os
import json
import sys
from typing import Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# 配置
TG_API_BASE    = os.getenv('TG_API_BASE', 'http://telegram-bot-api:8081')
TG_PUBLIC_BASE = 'https://api.telegram.org'  # 公共 API，回退用
CF_WORKER_URL  = os.getenv('CF_WORKER_URL', '')
CF_API_KEY     = os.getenv('CF_API_KEY', '')
CF_JWT         = ''

# ── Bot token 池 ──
_BOT_TOKENS: list[str] = []

try:
    _pool_raw = os.getenv('BOT_POOL', '')
    if _pool_raw:
        _pool = json.loads(_pool_raw)
        _BOT_TOKENS = [b.get('token', '') for b in _pool]
except Exception:
    pass

if not any(_BOT_TOKENS):
    for _i in range(10):
        _t = os.getenv(f'BOT{_i}_TOKEN', '')
        if _t:
            while len(_BOT_TOKENS) <= _i:
                _BOT_TOKENS.append('')
            _BOT_TOKENS[_i] = _t

if not any(_BOT_TOKENS):
    _fallback = os.getenv('BOT_TOKEN', '')
    if _fallback:
        _BOT_TOKENS = [_fallback]

_VALID_TOKENS: list[tuple[int, str]] = [
    (i, t) for i, t in enumerate(_BOT_TOKENS) if t
]


def _get_bot_token(bot_index: int) -> Optional[str]:
    if 0 <= bot_index < len(_BOT_TOKENS) and _BOT_TOKENS[bot_index]:
        return _BOT_TOKENS[bot_index]
    return None


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
        logger.warning(f'JWT 刷新失败：{e}')


async def _getfile_url(session: aiohttp.ClientSession, base: str, token: str, file_id: str) -> Optional[str]:
    """用指定 base 和 token 调用 getFile，返回完整下载 URL"""
    try:
        async with session.get(
            f'{base}/bot{token}/getFile',
            params={'file_id': file_id},
            timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            data = await resp.json()
            if data.get('ok'):
                return f'{base}/file/bot{token}/{data["result"]["file_path"]}'
    except Exception:
        pass
    return None


async def _fetch_file_url(
    session: aiohttp.ClientSession,
    file_id: str,
    preferred_bot_index: int
) -> Optional[str]:
    """
    下载 URL 获取策略：
      1. 尝试指定 bot + 本地容器
      2. 轮询其他 bot + 本地容器
      3. 全部本地失败 → 回退公共 API（每个 bot 轮询）
    """
    # 1. 优先 bot + 本地容器
    preferred = _get_bot_token(preferred_bot_index)
    if preferred:
        url = await _getfile_url(session, TG_API_BASE, preferred, file_id)
        if url:
            return url

    # 2. 其他 bot + 本地容器
    for idx, token in _VALID_TOKENS:
        if idx == preferred_bot_index:
            continue
        url = await _getfile_url(session, TG_API_BASE, token, file_id)
        if url:
            logger.info(f'  ↩ 本地容器 bot{idx} 找到 {file_id[:20]}...')
            return url

    # 3. 回退公共 API
    logger.info(f'  ↻ 本地容器均失败，尝试公共 API: {file_id[:20]}...')
    for idx, token in _VALID_TOKENS:
        url = await _getfile_url(session, TG_PUBLIC_BASE, token, file_id)
        if url:
            logger.info(f'  ✓ 公共 API bot{idx} 找到')
            return url

    logger.warning(f'所有方式均无法获取 {file_id[:20]}...')
    return None


async def _download_and_hash(
    session: aiohttp.ClientSession,
    file_ids: list[tuple[str, int]]
) -> Optional[str]:
    """流式下载多分片并计算 SHA-256"""
    h = hashlib.sha256()
    try:
        for file_id, bot_index in file_ids:
            download_url = await _fetch_file_url(session, file_id, bot_index)
            if not download_url:
                return None
            async with session.get(
                download_url, timeout=aiohttp.ClientTimeout(total=600)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f'下载失败 HTTP {resp.status} ({download_url[:60]}...)')
                    return None
                async for chunk in resp.content.iter_chunked(65536):
                    h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.warning(f'下载或计算异常：{e}')
        return None


async def _update_sha256(
    session: aiohttp.ClientSession,
    table: str,
    record_id: int,
    sha256: str,
    dry_run: bool
) -> bool:
    if dry_run:
        logger.info(f'  [dry-run] UPDATE {table} id={record_id} sha256={sha256[:16]}...')
        return True
    url = f'{CF_WORKER_URL}/api/admin/sha256-patch'
    payload = {'table': table, 'id': record_id, 'sha256': sha256}
    try:
        async with session.post(
            url, json=payload,
            headers={**_admin_headers(), 'Content-Type': 'application/json'},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 401:
                await _refresh_jwt(session)
                async with session.post(
                    url, json=payload,
                    headers={**_admin_headers(), 'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp2:
                    return resp2.status == 200
            return resp.status == 200
    except Exception as e:
        logger.warning(f'写回失败：{e}')
        return False


async def _fetch_null_records(session: aiohttp.ClientSession, limit: int) -> list[dict]:
    url = f'{CF_WORKER_URL}/api/admin/sha256-null-records'
    try:
        async with session.get(
            url, params={'limit': str(limit)},
            headers=_admin_headers(),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status == 401:
                await _refresh_jwt(session)
                async with session.get(
                    url, params={'limit': str(limit)},
                    headers=_admin_headers(),
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp2:
                    return (await resp2.json()).get('records', [])
            return (await resp.json()).get('records', [])
    except Exception as e:
        logger.error(f'获取记录失败：{e}')
        return []


async def process_record(
    session: aiohttp.ClientSession,
    rec: dict,
    dry_run: bool,
    sem: asyncio.Semaphore
):
    async with sem:
        table      = rec['table']
        rec_id     = rec['id']
        file_parts = rec.get('file_parts', '[]')
        file_name  = rec.get('file_name', '')

        try:
            parts = json.loads(file_parts)
        except Exception:
            logger.warning(f'[{table}#{rec_id}] file_parts 解析失败，跳过')
            return

        if not parts:
            logger.warning(f'[{table}#{rec_id}] file_parts 为空，跳过')
            return

        file_ids: list[tuple[str, int]] = []
        for part in parts:
            if isinstance(part, str):
                file_ids.append((part, 0))
            elif isinstance(part, dict):
                tg_file_id = part.get('id', '')
                bot_idx    = part.get('b', 0)
                if tg_file_id:
                    file_ids.append((tg_file_id, bot_idx))

        if not file_ids:
            logger.warning(f'[{table}#{rec_id}] 没有有效 file_id，跳过')
            return

        logger.info(f'[{table}#{rec_id}] 处理：{file_name} ({len(file_ids)} 分片)')

        sha256 = await _download_and_hash(session, file_ids)
        if not sha256:
            logger.warning(f'[{table}#{rec_id}] 计算 SHA-256 失败，跳过')
            return

        ok = await _update_sha256(session, table, rec_id, sha256, dry_run)
        status = '✅' if ok else '❌'
        logger.info(
            f'  {status} [{table}#{rec_id}] sha256={sha256[:16]}... '
            f'写回{"(dry-run)" if dry_run else ""}'
        )


async def main(dry_run: bool, limit: int, concurrency: int):
    if not CF_WORKER_URL or not CF_API_KEY:
        logger.error('缺少 CF_WORKER_URL 或 CF_API_KEY')
        sys.exit(1)
    if not _VALID_TOKENS:
        logger.error('未找到任何有效 Bot token')
        sys.exit(1)

    logger.info(f'已加载 {len(_VALID_TOKENS)} 个 Bot: {[f"bot{i}" for i, _ in _VALID_TOKENS]}')
    logger.info(f'本地容器: {TG_API_BASE} | 回退: {TG_PUBLIC_BASE}')

    connector = aiohttp.TCPConnector(limit=concurrency + 4)
    async with aiohttp.ClientSession(connector=connector) as session:
        await _refresh_jwt(session)

        logger.info(f'获取 sha256=NULL 的记录（limit={limit}）...')
        records = await _fetch_null_records(session, limit)
        logger.info(f'共 {len(records)} 条需要处理')

        if not records:
            logger.info('无需迁移记录，已完成。')
            return

        sem = asyncio.Semaphore(concurrency)
        tasks = [process_record(session, rec, dry_run, sem) for rec in records]
        await asyncio.gather(*tasks)

    logger.info(f'迁移完成！处理条数：{len(records)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SHA-256 历史数据回写迁移脚本')
    parser.add_argument('--dry-run', action='store_true', help='只打印不实际写入')
    parser.add_argument('--limit', type=int, default=500, help='最多处理条数（默认 500）')
    parser.add_argument('--concurrency', type=int, default=3, help='并发数（默认 3）')
    args = parser.parse_args()

    if args.dry_run:
        logger.info('=== DRY-RUN 模式 ===')

    asyncio.run(main(args.dry_run, args.limit, args.concurrency))
