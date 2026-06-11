import httpx
import os
from config import config

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

async def _post_import(metadata: dict, file_id: str, file_size: int, fname: str, bot_index: int = 0) -> dict:
    payload = {
        "title":       metadata.get("title", fname),
        "category":    metadata.get("category", config.DEFAULT_CATEGORY),
        "lang":        metadata.get("lang", "zh"),
        "description": metadata.get("description", ""),
        "file_name":   fname,
        "file_size":   file_size,
        "mime_type":   "audio/mpeg",
        "file_id":     file_id,
        "folder_id":   metadata.get("folder_id"),
        "bot_index":   bot_index,
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

async def direct_upload(file_path: str, metadata: dict) -> dict:
    fname = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    async with httpx.AsyncClient(timeout=600) as client:
        with open(file_path, "rb") as f:
            resp = await client.post(
                f"{config.TG_API_BASE}/bot{config.BOT_TOKEN}/sendAudio",
                data={
                    "chat_id":   config.STORAGE_CHAT_ID,
                    "title":     metadata.get("title", fname),
                    "performer": metadata.get("artist", ""),
                    "caption":   f"\U0001f3b5 {metadata.get('title', fname)}",
                },
                files={"audio": (fname, f, "audio/mpeg")}
            )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise Exception(f"TG \u4e0a\u4f20\u5931\u8d25\uff1a{data.get('description', 'unknown error')}")

    audio   = data["result"]["audio"]
    file_id = audio["file_id"]
    tg_size = audio.get("file_size", file_size)

    return await _post_import(metadata, file_id, tg_size, fname, bot_index=config.BOT_INDEX)
