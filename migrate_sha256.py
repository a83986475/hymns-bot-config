#!/usr/bin/env python3
"""
SHA-256 历史数据回写迁移脚本

下载策略：对每个分片，按以下顺序逐一尝试，成功即停：
  1. preferred bot  + 公共 api.telegram.org
  2. preferred bot  + 本地容器
  3. 其他 bot[0]  + 公共 api.telegram.org
  4. 其他 bot[0]  + 本地容器
  ... 以此类推
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

TG_LOCAL_BASE  = os.getenv('TG_API_BASE', 'http://telegram-bot-api:8081')
TG_PUBLIC_BASE = 'https://api.telegram.org'
CF_WORKER_URL  = os.getenv('CF_WORKER_URL', '')
CF_API_KEY     = os.getenv('CF_API_KEY', '')
CF_JWT         = ''

# ── 构建 Bot token 池 ──
_BOT_TOKENS: list[str] = []
try:
    _pool_raw = os.getenv('BOT_POOL', '')
    if _pool_raw:
        _BOT_TOKENS = [b.get('token', '') for b in json.loads(_pool_raw)]
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
    _t = os.getenv('BOT_TOKEN', '')
    if _t:
        _BOT_TOKENS = [_t]

_VALID_TOKENS: list[tuple[int, str]] = [
    (i, t) for i, t in enumerate(_BOT_TOKENS) if t
]


def _get_bot_token(idx: int) -> Optional[str]:
    if 0 <= idx < len(_BOT_TOKENS) and _BOT_TOKENS[idx]:
        return _BOT_TOKENS[idx]
    return None


def _admin_headers() -> dict:
    return {'X-Admin-Token': CF_API_KEY, 'Authorization': f'Bearer {CF_JWT}'}


async def _refresh_jwt(session: aiohttp.ClientSession):
    global CF_JWT
    try:
        async with session.post(
            f'{CF_WORKER_URL}/api/admin/login',
            json={'token': CF_API_KEY},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            CF_JWT = (await resp.json()).get('sessionToken', '')
            if CF_JWT:
                logger.info('JWT 已刷新')
    except Exception as e:
        logger.warning(f'JWT 刷新失败：{e}')


async def _try_download(
    session: aiohttp.ClientSession,
    base: str,
    token: str,
    file_id: str
) -> Optional[str]:
    """getFile + HEAD 验证，返回可用的下载 URL"""
    try:
        async with session.get(
            f'{base}/bot{token}/getFile',
            params={'file_id': file_id},
            timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            data = await resp.json()
            if not data.get('ok'):
                return None
            url = f'{base}/file/bot{token}/{data["result"]["file_path"]}'

        async with session.head(
            url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True
        ) as head:
            if head.status == 200:
                return url
            return None
    except Exception:
        return None


async def _fetch_file_url(
    session: aiohttp.ClientSession,
    file_id: str,
    preferred_bot_index: int
) -> Optional[str]:
    """preferred bot 排首，对每个 bot 先公共 API 再本地容器"""
    ordered: list[tuple[int, str]] = []
    preferred = _get_bot_token(preferred_bot_index)
    if preferred:
        ordered.append((preferred_bot_index, preferred))
    for idx, token in _VALID_TOKENS:
        if idx != preferred_bot_index:
            ordered.append((idx, token))

    for bot_idx, token in ordered:
        url = await _try_download(session, TG_PUBLIC_BASE, token, file_id)
        if url:
            logger.debug(f'  公共API bot{bot_idx} OK: {file_id[:16]}...')
            return url
        url = await _try_download(session, TG_LOCAL_BASE, token, file_id)
        if url:
            logger.debug(f'  本地 bot{bot_idx} OK: {file_id[:16]}...')
            return url

    logger.warning(f'  所有尝试均失败: {file_id[:20]}...')
    return None


async def _download_and_hash(
    session: aiohttp.ClientSession,
    file_ids: list[tuple[str, int]]
) -> Optional[str]:
    h = hashlib.sha256()
    try:
        for file_id, bot_index in file_ids:
            url = await _fetch_file_url(session, file_id, bot_index)
            if not url:
                return None
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as resp:
                if resp.status != 200:
                    logger.warning(f'  下载 HTTP {resp.status}: {url[:60]}')
                    return None
                async for chunk in resp.content.iter_chunked(65536):
                    h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        logger.warning(f'  下载异常: {e}')
        return None


async def _update_sha256(
    session: aiohttp.ClientSession,
    table: str, record_id: int, sha256: str, dry_run: bool
) -> bool:
    if dry_run:
        logger.info(f'  [dry-run] {table}#{record_id} sha256={sha256[:16]}...')
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
                ) as r2:
                    return r2.status == 200
            return resp.status == 200
    except Exception as e:
        logger.warning(f'  写回失败: {e}')
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
                ) as r2:
                    return (await r2.json()).get('records', [])
            return (await resp.json()).get('records', [])
    except Exception as e:
        logger.error(f'获取记录失败: {e}')
        return []


async def process_record(
    session: aiohttp.ClientSession,
    rec: dict, dry_run: bool, sem: asyncio.Semaphore
):
    async with sem:
        table     = rec['table']
        rec_id    = rec['id']
        file_name = rec.get('file_name', '')
        try:
            parts = json.loads(rec.get('file_parts', '[]'))
        except Exception:
            logger.warning(f'[{table}#{rec_id}] file_parts 解析失败')
            return

        if not parts:
            logger.warning(f'[{table}#{rec_id}] file_parts 为空')
            return

        file_ids: list[tuple[str, int]] = []
        for p in parts:
            if isinstance(p, str):
                file_ids.append((p, 0))
            elif isinstance(p, dict) and p.get('id'):
                file_ids.append((p['id'], p.get('b', 0)))

        if not file_ids:
            logger.warning(f'[{table}#{rec_id}] 没有有效 file_id')
            return

        logger.info(f'[{table}#{rec_id}] {file_name} ({len(file_ids)} 分片)')
        sha256 = await _download_and_hash(session, file_ids)
        if not sha256:
            logger.warning(f'  ✖ [{table}#{rec_id}] 失败，跳过')
            return

        ok = await _update_sha256(session, table, rec_id, sha256, dry_run)
        icon = '✅' if ok else '❌'
        tag = '(dry-run)' if dry_run else ''
        logger.info(f'  {icon} [{table}#{rec_id}] {sha256[:16]}... {tag}')


async def main(dry_run: bool, limit: int, concurrency: int):
    if not CF_WORKER_URL or not CF_API_KEY:
        logger.error('缺少 CF_WORKER_URL 或 CF_API_KEY')
        sys.exit(1)
    if not _VALID_TOKENS:
        logger.error('未找到任何有效 Bot token')
        sys.exit(1)

    logger.info(f'已加载 {len(_VALID_TOKENS)} 个 Bot: {["bot" + str(i) for i, _ in _VALID_TOKENS]}')
    logger.info(f'公共API: {TG_PUBLIC_BASE} | 本地: {TG_LOCAL_BASE}')

    connector = aiohttp.TCPConnector(limit=concurrency * 4)
    async with aiohttp.ClientSession(connector=connector) as session:
        await _refresh_jwt(session)
        logger.info(f'获取 sha256=NULL 记录（limit={limit}）...')
        records = await _fetch_null_records(session, limit)
        logger.info(f'共 {len(records)} 条需要处理')
        if not records:
            logger.info('无需迁移记录，已完成。')
            return
        sem = asyncio.Semaphore(concurrency)
        await asyncio.gather(*[process_record(session, r, dry_run, sem) for r in records])

    logger.info(f'迁移完成！处理条数: {len(records)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SHA-256 历史数据回写迁移脚本')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=500)
    parser.add_argument('--concurrency', type=int, default=3)
    args = parser.parse_args()
    if args.dry_run:
        logger.info('=== DRY-RUN 模式 ===')
    asyncio.run(main(args.dry_run, args.limit, args.concurrency))
