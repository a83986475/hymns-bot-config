import asyncio
import logging
import subprocess
import httpx
import math
import os
import random
import tempfile
from config import config

logger = logging.getLogger(__name__)

# 分片大小：18MB
# 实际受限于 Telegram Bot API 本地代理（telegram-bot-api）默认 20MB 上传上限，
# 因此单分片不能过于接近该值。
# 18MB 与前端本地上传的分片上限保持一致，
# 且留出约 2MB 的 multipart/form-data 开销余量，避免因元数据导致 413 拒绝。
CHUNK_SIZE = 18 * 1024 * 1024


async def refresh_jwt() -> str:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{config.CF_WORKER_URL}/api/admin/login",
                    json={"token": config.CF_API_KEY}
                )
                jwt = resp.json().get("sessionToken", "")
                if jwt:
                    config.CF_JWT = jwt
                return jwt
        except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError, httpx.TimeoutException) as e:
            if attempt < max_retries - 1:
                delay = 2 ** attempt
                logger.warning(f'JWT 刷新网络错误（{type(e).__name__}），{delay}s 后第 {attempt+2} 次重试')
                await asyncio.sleep(delay)
                continue
            logger.error(f'JWT 刷新重试 {max_retries} 次后仍失败: {e}')
            raise
    return ""


def _admin_headers() -> dict:
    headers = {"X-Admin-Token": config.CF_API_KEY}
    if config.CF_JWT:
        headers["Authorization"] = f"Bearer {config.CF_JWT}"
    return headers


async def check_duplicate(sha256: str, file_name: str, file_size: int) -> dict | None:
    """调用 Worker 去重检测接口，返回已存在的记录或 None"""
    if not config.CF_WORKER_URL:
        return None
    params = {"hash": sha256, "name": file_name, "size": str(file_size)}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{config.CF_WORKER_URL}/api/files/check-duplicate",
                params=params,
                headers=_admin_headers(),
            )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("exists"):
                return data
    except Exception:
        pass
    return None


async def _post_import(metadata: dict, file_parts: list, file_size: int, fname: str) -> dict:
    """调用 Worker import 接口写入 D1 记录，file_parts 为分片列表 [{"id": "tg_file_id", "b": bot_index}, ...]"""
    mime_type = metadata.get("mime_type", "audio/mpeg")
    payload = {
        "title":       metadata.get("title", fname),
        "category":    metadata.get("category", config.DEFAULT_CATEGORY),
        "lang":        metadata.get("lang", "zh"),
        "description": metadata.get("description", ""),
        "file_name":   fname,
        "file_size":   file_size,
        "mime_type":   mime_type,
        "file_parts":  file_parts,
        "folder_id":   metadata.get("folder_id"),
        "sha256":      metadata.get("sha256"),
        "uploader_id": metadata.get("uploader_id"),
    }
    async def _do_import():
        """执行 import HTTP 请求，网络错误自动重试最多 3 次。"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    return await client.post(
                        f"{config.CF_WORKER_URL}/api/hymns/import",
                        headers={**_admin_headers(), "Content-Type": "application/json"},
                        json=payload
                    )
            except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError, httpx.TimeoutException) as e:
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning(f'Worker import 网络错误（{type(e).__name__}），{delay}s 后第 {attempt+2} 次重试')
                    await asyncio.sleep(delay)
                    continue
                raise

    resp = await _do_import()
    if resp.status_code == 401:
        await refresh_jwt()
        resp = await _do_import()
    if resp.status_code != 200:
        body = resp.text[:500]
        logger.error(f'Worker import 失败 ({resp.status_code}): {body}')
    resp.raise_for_status()
    return resp.json()


async def _get_upload_bot_token() -> dict:
    """从 Worker BotPool 获取一个上传用的 bot token（轮询分配）。返回 {token, bot_index}，失败时回退到自身。"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{config.CF_WORKER_URL}/api/bot/next-upload-token",
                headers=_admin_headers()
            )
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        pass
    # 回退：使用自己的 token
    return {"token": config.BOT_TOKEN, "bot_index": config.BOT_INDEX}


async def _tg_upload_chunk(chunk_data: bytes, chunk_name: str, mime_type: str, is_video: bool, caption: str = None, bot_token: str = None, bot_index: int = None) -> dict:
    """上传单个分片到 Telegram，返回 file_id 和 bot_index"""
    _token = bot_token or config.BOT_TOKEN
    _bot_index = bot_index if bot_index is not None else config.BOT_INDEX
    data = {"chat_id": config.STORAGE_CHAT_ID}
    if caption:
        data["caption"] = caption

    if is_video:
        field = "document"
        url = f"{config.TG_API_BASE}/bot{_token}/sendDocument"
    else:
        field = "audio"
        data["title"] = chunk_name
        url = f"{config.TG_API_BASE}/bot{_token}/sendAudio"

    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Telegram 上传直连，不经过代理（代理对大文件上传不稳定）
            async with httpx.AsyncClient(proxy=None, timeout=600) as client:
                files = {field: (chunk_name, chunk_data, mime_type)}
                resp = await client.post(url, data=data, files=files)

            if resp.status_code == 429:
                retry_after = 5 * (2 ** attempt) + random.uniform(0, 3)
                import logging
                logging.getLogger(__name__).warning(
                    f'TG 429 限流，{retry_after:.1f}s 后重试 (attempt {attempt+1}/{max_retries})'
                )
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code == 413:
                # 分片过大，缩小分片无意义，直接报错
                raise Exception(
                    f'TG 413 Payload Too Large：分片 {chunk_name} ({len(chunk_data)} bytes) '
                    '超过 Telegram Bot API 上传上限（50MB），请减小 CHUNK_SIZE'
                )

            resp.raise_for_status()
            result = resp.json()
            if not result.get("ok"):
                raise Exception(f"TG 上传失败：{result.get('description', 'unknown error')}")

            if is_video:
                msg = result["result"]
                file_id = None
                if "document" in msg and msg["document"]:
                    file_id = msg["document"].get("file_id")
                if not file_id and "video" in msg and msg["video"]:
                    file_id = msg["video"].get("file_id")
                if not file_id:
                    raise Exception(f"TG 返回中没有 file_id (document/video 都缺失): {list(msg.keys())}")
                return {"file_id": file_id, "b": _bot_index}
            else:
                return {"file_id": result["result"]["audio"]["file_id"], "b": _bot_index}

        except Exception as e:
            if attempt < max_retries - 1:
                import logging
                logging.getLogger(__name__).warning(
                    f'分片 {chunk_name} 上传失败 (attempt {attempt+1}/{max_retries}): {e}，重试...'
                )
                await asyncio.sleep(2 ** attempt)
                continue
            raise

    raise Exception(f"分片 {chunk_name} 上传重试 {max_retries} 次后仍失败")


async def direct_upload(file_path: str, metadata: dict, uploader_id: int = None, skip_import: bool = False) -> dict:
    """
    直连模式：Bot 直接把文件分片上传到 Telegram Cloud API。
    当 skip_import=True 时只上传到 Telegram CDN，不创建 D1 记录。
    文件 > 10MB 时自动分片，每片单独 sendDocument/sendAudio。
    429 自动重试（递增退避 + 随机 jitter）。
    """
    return await _do_upload(file_path, metadata, uploader_id, skip_import)


async def _do_upload(file_path: str, metadata: dict, uploader_id: int = None, skip_import: bool = False) -> dict:
    fname = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    sha256 = metadata.get("sha256")
    mime_type = metadata.get("mime_type", "audio/mpeg")
    is_video = mime_type and mime_type.startswith("video/")

    # ── MP4 faststart：将 moov atom 移到文件开头，优化浏览器播放启动速度 ──
    # 仅对 MP4 视频文件执行（使用 ffmpeg -c copy 无需重新编码，仅移动元数据）
    if is_video and fname.lower().endswith('.mp4'):
        import logging
        logger = logging.getLogger(__name__)
        try:
            import subprocess
            import tempfile
            # 用临时文件存储处理后的视频
            tmp_fd, tmp_path = tempfile.mkstemp(suffix='.mp4')
            os.close(tmp_fd)
            result = subprocess.run(
                ['ffmpeg', '-i', file_path, '-c', 'copy', '-movflags', '+faststart', '-y', tmp_path],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                # 用处理后的文件替换原文件
                orig_path = file_path
                os.remove(orig_path)
                os.rename(tmp_path, orig_path)
                # 更新文件大小
                file_size = os.path.getsize(orig_path)
                logger.info(f'faststart 优化完成: {fname}')
            else:
                logger.warning(f'faststart 失败 (returncode={result.returncode}): {result.stderr[:200]}')
                # 清理临时文件
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        except FileNotFoundError:
            # ffmpeg 不可用，跳过 faststart
            pass
        except subprocess.TimeoutExpired:
            logger.warning(f'faststart 超时: {fname}')
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        except Exception as e:
            logger.warning(f'faststart 处理失败: {e}')
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    # 秒传检测：有 sha256 才检测，命中则跳过 TG 上传
    # skip_import=True（TG CDN 模式）时跳过，因为需要返回 file_parts 供 Worker 代理下载
    if not skip_import and sha256:
        dup = await check_duplicate(sha256, fname, file_size)
        if dup:
            return {"id": dup.get("id"), "dedup": True, "filename": dup.get("filename")}

    file_parts = []

    if file_size <= CHUNK_SIZE:
        # ── 小文件：直接上传，不分片 ──
        caption = (
            f"\U0001f3ac {metadata.get('title', fname)}"
            if is_video
            else f"\U0001f3b5 {metadata.get('title', fname)}"
        )
        with open(file_path, "rb") as f:
            chunk_data = f.read()
        # 从 BotPool 获取上传 token（分布式上传）
        token_data = await _get_upload_bot_token()
        result = await _tg_upload_chunk(chunk_data, fname, mime_type, is_video, caption,
                                        token_data["token"], token_data["bot_index"])
        file_parts.append({"id": result["file_id"], "b": result.get("b", token_data["bot_index"])})
    else:
        # ── 大文件：分片串行上传 ──
        # 串行上传：每个分片逐个上传，_tg_upload_chunk 内部已含 5 次重试
        # 避免并行上传时一个分片失败导致 asyncio.gather 取消其余分片，产生孤儿分片
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        with open(file_path, "rb") as f:
            for i in range(total_chunks):
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break
                chunk_name = f"{fname}.part{i + 1}of{total_chunks}"
                caption = (
                    f"\U0001f3ac {metadata.get('title', fname)} [part 1/{total_chunks}]"
                    if i == 0 and is_video
                    else (f"\U0001f3b5 {metadata.get('title', fname)} [part 1/{total_chunks}]"
                          if i == 0 else None)
                )
                token = await _get_upload_bot_token()
                result = await _tg_upload_chunk(
                    chunk_data, chunk_name, mime_type, is_video, caption,
                    token["token"], token["bot_index"]
                )
                file_parts.append({"id": result["file_id"], "b": result.get("b", token["bot_index"])})

    if uploader_id is not None:
        metadata["uploader_id"] = uploader_id

    if skip_import:
        return {
            "file_parts": file_parts,
            "file_name": fname,
            "file_size": file_size,
            "title": metadata.get("title", fname),
            "duration": metadata.get("duration", 0),
            "mime_type": mime_type,
        }

    return await _post_import(metadata, file_parts, file_size, fname)
