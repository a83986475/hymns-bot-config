import yt_dlp
import os
from config import config

os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)

COOKIE_FILE = os.path.join(os.path.dirname(__file__), 'cookies.txt')
POT_PROVIDER_URL = 'http://bgutil-ytdlp-pot-provider:4416'


def _base_opts() -> dict:
    opts = {
        'quiet': True,
        'no_warnings': True,
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
        if h and f.get('vcodec', 'none') != 'none' and h not in seen:
            seen.add(h)
            video_formats.append({'height': h, 'format_id': f['format_id'], 'ext': f.get('ext', 'mp4')})
    common = {360, 480, 720, 1080}
    video_formats = [f for f in video_formats if f['height'] in common]
    video_formats.sort(key=lambda x: x['height'])

    return {
        'title': info.get('title', ''),
        'duration': info.get('duration', 0),
        'uploader': info.get('uploader', ''),
        'description': (info.get('description') or '')[:500],
        'video_formats': video_formats,
    }


def download_audio(url: str) -> dict:
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
        }


def download_video(url: str, format_id: str) -> dict:
    ydl_opts = {
        **_base_opts(),
        'format': f'{format_id}+bestaudio/best',
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
        }
