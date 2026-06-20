import asyncio
import httpx
import math
import os
from config import config

# 每 bot 最多 1 个并发上传，防止 Telegram flood control
_upload_semaphore = asyncio.Semaphore(1)

# 分片大小：50MB，减少分片数量以降低 Telegram API 调用次数和上传失败概率
# 同时仍远低于 Telegram Cloud API 的 20MB getFile 限制（本地容器无此限制）
CHUNK_SIZE = 50 * 1024 * 1024


async def refresh_jwt() -> str:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{config.CF_WORKER_URL}/api/admin/login",
            json={"token": config.CF_API_KEY}
        )
        jwt = resp.json().get("sessionToken", "")
        if jwt:
            config.CF_JWT = jwt
        return jwt


def _admin_headers() -> dict:
    return {
        "X-Admin-Token":  config.CF_API_KEY,
        "Authorization":  f"Bearer {config.CF_JWT}",
    }


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
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{config.CF_WORKER_URL}/api/hymns/import",
            headers={**_admin_headers(), "Content-Type": "application/json"},
            json=payload
        )
    if resp.status_code == 401:
        await refresh_jwt()
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{config.CF_WORKER_URL}/api/hymns/import",
                headers={**_admin_headers(), "Content-Type": "application/json"},
                json=payload
            )
    resp.raise_for_status()
    return resp.json()


async def _tg_upload_chunk(chunk_data: bytes, chunk_name: str, mime_type: str, is_video: bool, caption: str = None) -> dict:
    """上传单个分片到 Telegram，返回 file_id"""
    data = {"chat_id": config.STORAGE_CHAT_ID}
    if caption:
        data["caption"] = caption

    if is_video:
        field = "document"
        url = f"{config.TG_API_BASE}/bot{config.BOT_TOKEN}/sendDocument"
    else:
        field = "audio"
        data["title"] = chunk_name
        url = f"{config.TG_API_BASE}/bot{config.BOT_TOKEN}/sendAudio"

    max_retries = 5
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=600) as client:
                files = {field: (chunk_name, chunk_data, mime_type)}
                resp = await client.post(url, data=data, files=files)

            if resp.status_code == 429:
                retry_after = 5 * (2 ** attempt)
                import logging
                logging.getLogger(__name__).warning(
                    f'TG 429 限流，{retry_after}s 后重试 (attempt {attempt+1}/{max_retries})'
                )
                await asyncio.sleep(retry_after)
                continue

            resp.raise_for_status()
            result = resp.json()
            if not result.get("ok"):
                raise Exception(f"TG 上传失败：{result.get('description', 'unknown error')}")

            if is_video:
                return {"file_id": result["result"]["document"]["file_id"]}
            else:
                return {"file_id": result["result"]["audio"]["file_id"]}

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


async def direct_upload(file_path: str, metadata: dict, uploader_id: int = None) -> dict:
    """
    直连模式：Bot 直接把文件分片上传到 Telegram Cloud API，
    然后只调用 Worker 写一条 D1 记录。
    文件 > 10MB 时自动分片，每片单独 sendDocument/sendAudio。
    使用信号量限制并发 + 429 自动重试。
    """
    async with _upload_semaphore:
        return await _do_upload(file_path, metadata, uploader_id)


async def _do_upload(file_path: str, metadata: dict, uploader_id: int = None) -> dict:
    fname = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    sha256 = metadata.get("sha256")
    mime_type = metadata.get("mime_type", "audio/mpeg")
    is_video = mime_type and mime_type.startswith("video/")

    # 秒传检测：有 sha256 才检测，命中则跳过 TG 上传
    if sha256:
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
        result = await _tg_upload_chunk(chunk_data, fname, mime_type, is_video, caption)
        file_parts.append({"id": result["file_id"], "b": config.BOT_INDEX})
    else:
        # ── 大文件：分片上传 ──
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        with open(file_path, "rb") as f:
            for i in range(total_chunks):
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break
                chunk_name = f"{fname}.part{i + 1}of{total_chunks}"
                # 只有第一片带 caption
                caption = (
                    f"\U0001f3ac {metadata.get('title', fname)} [part 1/{total_chunks}]"
                    if i == 0 and is_video
                    else (f"\U0001f3b5 {metadata.get('title', fname)} [part 1/{total_chunks}]"
                          if i == 0 else None)
                )
                result = await _tg_upload_chunk(chunk_data, chunk_name, mime_type, is_video, caption)
                file_parts.append({"id": result["file_id"], "b": config.BOT_INDEX})

    if uploader_id is not None:
        metadata["uploader_id"] = uploader_id
    return await _post_import(metadata, file_parts, file_size, fname)
