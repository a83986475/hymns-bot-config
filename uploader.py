import asyncio
import httpx
import os
from config import config

# 每 bot 最多 1 个并发上传，防止 Telegram flood control
_upload_semaphore = asyncio.Semaphore(1)


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


async def _post_import(metadata: dict, file_id: str, file_size: int, fname: str, bot_index: int = 0) -> dict:
    mime_type = metadata.get("mime_type", "audio/mpeg")
    payload = {
        "title":       metadata.get("title", fname),
        "category":    metadata.get("category", config.DEFAULT_CATEGORY),
        "lang":        metadata.get("lang", "zh"),
        "description": metadata.get("description", ""),
        "file_name":   fname,
        "file_size":   file_size,
        "mime_type":   mime_type,
        "file_id":     file_id,
        "folder_id":   metadata.get("folder_id"),
        "bot_index":   bot_index,
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


async def _tg_upload_with_retry(url: str, data: dict, file_path: str, file_field: str, mime_type: str, max_retries: int = 5) -> dict:
    """向 Telegram API 上传文件，遇到 429 RetryAfter 自动重试（每次重试重新打开文件）"""
    fname = os.path.basename(file_path)
    for attempt in range(max_retries):
        with open(file_path, "rb") as f:
            async with httpx.AsyncClient(timeout=600) as client:
                files = {file_field: (fname, f, mime_type)}
                resp = await client.post(url, data=data, files=files)

        if resp.status_code == 429:
            retry_after = 5 * (2 ** attempt)  # 5s, 10s, 20s, 40s, 80s
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
        return result

    raise Exception(f"TG 上传重试 {max_retries} 次后仍失败")


async def direct_upload(file_path: str, metadata: dict, uploader_id: int = None) -> dict:
    """
    直连模式：Bot 直接把文件发到本地 TG Bot API Server（无大小限制）
    然后只调用 Worker 写一条 D1 记录
    使用信号量限制并发 + 429 自动重试
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

    if is_video:
        data = {
            "chat_id": config.STORAGE_CHAT_ID,
            "caption": f"\U0001f3ac {metadata.get('title', fname)}",
        }
        result = await _tg_upload_with_retry(
            f"{config.TG_API_BASE}/bot{config.BOT_TOKEN}/sendDocument",
            data=data,
            file_path=file_path,
            file_field="document",
            mime_type=mime_type,
        )
        doc = result["result"].get("document", {})
        file_id = doc["file_id"]
        tg_size = doc.get("file_size", file_size)
    else:
        data = {
            "chat_id": config.STORAGE_CHAT_ID,
            "title": metadata.get("title", fname),
            "performer": metadata.get("artist", ""),
            "caption": f"\U0001f3b5 {metadata.get('title', fname)}",
        }
        result = await _tg_upload_with_retry(
            f"{config.TG_API_BASE}/bot{config.BOT_TOKEN}/sendAudio",
            data=data,
            file_path=file_path,
            file_field="audio",
            mime_type=mime_type,
        )
        audio = result["result"]["audio"]
        file_id = audio["file_id"]
        tg_size = audio.get("file_size", file_size)

    if uploader_id is not None:
        metadata["uploader_id"] = uploader_id
    return await _post_import(metadata, file_id, tg_size, fname, bot_index=config.BOT_INDEX)
