import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
load_dotenv()

@dataclass
class Config:
    BOT_TOKEN:        str  = os.getenv("BOT_TOKEN", "")
    CF_WORKER_URL:    str  = os.getenv("CF_WORKER_URL", "")
    CF_API_KEY:       str  = os.getenv("CF_API_KEY", "")
    CF_JWT:           str  = os.getenv("CF_JWT", "")
    DOWNLOAD_DIR:     str  = "/tmp/hymns"
    DEFAULT_CATEGORY: str  = os.getenv("DEFAULT_CATEGORY", "诗歌音频")
    ADMIN_IDS:        list = field(default_factory=list)
    STORAGE_CHAT_ID:  str  = os.getenv("STORAGE_CHAT_ID", "")
    TG_API_BASE:      str  = os.getenv("TG_API_BASE", "http://telegram-bot-api:8081")

    def __post_init__(self):
        ids = os.getenv("ADMIN_IDS", "")
        self.ADMIN_IDS = [int(i) for i in ids.split(",") if i.strip()]

config = Config()
