import yt_dlp
import os
import hashlib
from config import config

os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)

COOKIE_FILE = os.path.join(os.path.dirname(__file__), 'cookies.txt')
POT_PROVIDER_URL = 'http://bgutil-ytdlp-pot-provider:4416'


def _base_opts() -> dict:
    opts = {
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'js_runtimes': {'node': {}},
        'extractor_args': {
            'youtube': {
                'player_client': ['web', 'mweb'],
            },
            'youtubepot-bgutilhttp': {
                'base_url': [POT_PROVIDER_URL],
            },
        },
    }
    if os.path.exists(COOKIE_FILE):
        opts['cookiefile'] = COOKIE_FILE
    return opts


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def search_youtube(keyword: str, max_results: int = 5) -> list:
    ydl_opts = {**_base_opts(), 'extract_flat': True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(f'ytsearch{max_results}:{keyword}', download=False)
        entries = result.get('entries', [])
        return [
            {
                'index': i + 1,
                'title': e.get('title', '未知'),
                'url': f"https://youtube.com/watch?v={e.get('id')}",
                'duration': e.get('duration', 0),
                'uploader': e.get('uploader', ''),
            }
            for i, e in enumerate(entries)
        ]


# 支持的分辨率白名单（最低 480p，不显示 360p 及以下）
SUPPORTED_HEIGHTS = {480, 720, 1080, 1440, 2160, 4320}

# 分辨率标签映射
HEIGHT_LABELS = {
    2160: '2160p (4K)',
    4320: '4320p (8K)',
}


def get_formats(url: str) -> dict:
    ydl_opts = {
        **_base_opts(),
        'skip_download': True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    seen = set()
    video_formats = []
    for f in reversed(info.get('formats', [])):
        h = f.get('height')
        if h and f.get('vcodec', 'none') != 'none' and h not in seen and h in SUPPORTED_HEIGHTS:
            seen.add(h)
            video_formats.append({'height': h, 'format_id': f['format_id'], 'ext': f.get('ext', 'mp4')})
    video_formats.sort(key=lambda x: x['height'])

    return {
        'title': info.get('title', ''),
        'duration': info.get('duration', 0),
        'uploader': info.get('uploader', ''),
        'description': (info.get('description') or '')[:500],
        'video_formats': video_formats,
    }


def get_playlist_info(url: str) -> dict:
    ydl_opts = {
        **_base_opts(),
        'extract_flat': True,
        'noplaylist': False,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    entries = info.get('entries', [])
    total_duration = sum(e.get('duration') or 0 for e in entries)
    return {
        'title': info.get('title', ''),
        'count': len(entries),
        'total_duration': total_duration,
        'entries': [
            {
                'index': i + 1,
                'title': e.get('title', '未知'),
                'url': f"https://youtube.com/watch?v={e.get('id')}",
                'duration': e.get('duration', 0),
            }
            for i, e in enumerate(entries)
        ],
    }


def download_audio(url: str) -> dict:
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
    ydl_opts = {
        **_base_opts(),
        'format': 'bestaudio/best',
        'outtmpl': f'{config.DOWNLOAD_DIR}/%(id)s.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        file_path = f"{config.DOWNLOAD_DIR}/{info['id']}.mp3"
        return {
            'file_path': file_path,
            'title': info.get('title', ''),
            'artist': info.get('uploader', ''),
            'duration': info.get('duration', 0),
            'source_url': url,
            'description': (info.get('description') or '')[:500],
            'category': config.DEFAULT_CATEGORY,
            'mime_type': 'audio/mpeg',
            'sha256': _sha256_file(file_path),
        }


def download_video(url: str, format_id: str) -> dict:
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
    # format_id 处理：支持预设值（''/''best''/''1080''/''720''）或具体 format_id
    if not format_id or format_id in ('best', ''):
        format_str = 'bestvideo+bestaudio/best'
    elif format_id == '1080':
        format_str = 'bestvideo[height<=1080]+bestaudio/best'
    elif format_id == '720':
        format_str = 'bestvideo[height<=720]+bestaudio/best'
    elif '+' in format_id:
        format_str = format_id
    else:
        format_str = f'{format_id}+bestaudio/best'
    ydl_opts = {
        **_base_opts(),
        'format': format_str,
        'outtmpl': f'{config.DOWNLOAD_DIR}/%(id)s.%(ext)s',
        'merge_output_format': 'mp4',
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        file_path = f"{config.DOWNLOAD_DIR}/{info['id']}.mp4"
        return {
            'file_path': file_path,
            'title': info.get('title', ''),
            'artist': info.get('uploader', ''),
            'duration': info.get('duration', 0),
            'source_url': url,
            'description': (info.get('description') or '')[:500],
            'category': config.DEFAULT_CATEGORY,
            'mime_type': 'video/mp4',
            'sha256': _sha256_file(file_path),
        }
