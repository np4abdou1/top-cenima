"""
TopCinema Scraper with Flask Web Dashboard - FIXED VERSION
Fixes:
1. Anime type now stored as 'anime' instead of 'series'
2. Trailer fetching improved for series/anime
3. Database viewer shows actual data with pagination
4. Download scraper.db functionality added
5. Enhanced stats page with more metrics
"""
import json
import os
import re
import time
import sqlite3
import threading
import logging
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue  # Added Queue for buffering fetched shows

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, Response, request, send_file

# --- Global State Management ---

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

GLOBAL_STATE: Dict[str, Any] = {
    "scraper_running": False,
    "stop_scraper": False,
    "log_message": "Scraper is idle. Press 'Start' to begin.",
    "log_buffer": [],  # New: accumulate logs
    "stats": {
        "total_sources": 0,
        "total_pending": 0,
        "completed": 0,
        "failed": 0,
        "series": 0,
        "movies": 0,
        "anime": 0,
        "current_file": "N/A",
        "failed_urls": []
    },
    "test_stats": {
        "fetching": 0,
        "in_queue": 0,
        "written": 0,
        "failed": 0
    },
    "json_files": ["data/series_animes.json", "data/movies.json"],
    "db_path": "data/scraper.db",
    "test_db_path": "data/test.db"
}
SCRAPER_THREAD = None

# --- Database Initialization ---

def init_database(db_path: str = "data/scraper.db"):
    """Create 4-table database schema with anime support"""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA foreign_keys = ON")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL UNIQUE,
        slug TEXT UNIQUE,
        type TEXT NOT NULL CHECK(type IN ('movie', 'series', 'anime')),
        poster TEXT,
        synopsis TEXT,
        imdb_rating REAL,
        trailer TEXT,
        year INTEGER,
        genres TEXT,
        cast TEXT,
        directors TEXT,
        country TEXT,
        language TEXT,
        duration TEXT,
        source_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_type ON shows(type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_slug ON shows(slug)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS seasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER NOT NULL,
        season_number INTEGER NOT NULL,
        poster TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (show_id) REFERENCES shows(id) ON DELETE CASCADE,
        UNIQUE(show_id, season_number)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_seasons_show ON seasons(show_id)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS episodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season_id INTEGER NOT NULL,
        episode_number INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (season_id) REFERENCES seasons(id) ON DELETE CASCADE,
        UNIQUE(season_id, episode_number)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_season ON episodes(season_id)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS servers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        episode_id INTEGER NOT NULL,
        server_number INTEGER NOT NULL,
        embed_url TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (episode_id) REFERENCES episodes(id) ON DELETE CASCADE
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_servers_episode ON servers(episode_id)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scrape_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'completed', 'failed')),
        show_id INTEGER,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (show_id) REFERENCES shows(id) ON DELETE SET NULL
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_status ON scrape_progress(status)")
    
    conn.commit()
    conn.close()

# --- Database Class ---

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    def slugify(self, text: str) -> str:
        text = text.lower().strip()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text)
        return text[:100]
    
    def insert_show(self, show_data: Dict) -> Optional[int]:
        """Insert show and return ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            title = show_data.get("title")
            slug = self.slugify(title)
            metadata = show_data.get("metadata", {})
            
            def to_string(value):
                if isinstance(value, list):
                    return ", ".join(str(v) for v in value if v)
                return str(value) if value else None
            
            year = show_data.get("year")
            if not year:
                year_str = metadata.get("release_year") or metadata.get("year")
                if year_str:
                    match = re.search(r'(\d{4})', str(year_str))
                    if match:
                        year = int(match.group(1))
            
            show_type = show_data.get("type", "series")

            cursor.execute("""
            INSERT INTO shows (title, slug, type, poster, synopsis, imdb_rating, trailer, year, 
                             genres, cast, directors, country, language, duration, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title, slug, show_type,
                show_data.get("poster"), show_data.get("synopsis"),
                show_data.get("imdb_rating"), show_data.get("trailer"), year,
                to_string(metadata.get("genres")), to_string(metadata.get("cast")),
                to_string(metadata.get("directors")), to_string(metadata.get("country")),
                to_string(metadata.get("language")), to_string(metadata.get("duration")),
                show_data.get("source_url")
            ))
            show_id = cursor.lastrowid
            conn.commit()
            return show_id
        except sqlite3.IntegrityError:
            log_message(f"Show '{show_data.get('title')}' already exists.", level="warning")
            return None
        except Exception as e:
            log_message(f"DB Insert Error: {e}", level="error")
            return None
        finally:
            conn.close()

    def insert_seasons_and_episodes(self, show_id: int, seasons: List[Dict]):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            for season in seasons:
                season_num = season.get("season_number", 1)
                season_poster = season.get("poster")
                
                cursor.execute("INSERT OR IGNORE INTO seasons (show_id, season_number, poster) VALUES (?, ?, ?)", 
                               (show_id, season_num, season_poster))
                
                season_id = cursor.lastrowid
                if season_id == 0:
                    cursor.execute("SELECT id FROM seasons WHERE show_id = ? AND season_number = ?", (show_id, season_num))
                    result = cursor.fetchone()
                    if result: season_id = result[0]
                
                if not season_id: continue

                for episode in season.get("episodes", []):
                    cursor.execute("INSERT OR IGNORE INTO episodes (season_id, episode_number) VALUES (?, ?)", 
                                   (season_id, episode.get("episode_number")))
                    
                    episode_id = cursor.lastrowid
                    if episode_id == 0:
                        cursor.execute("SELECT id FROM episodes WHERE season_id = ? AND episode_number = ?", (season_id, episode.get("episode_number")))
                        result = cursor.fetchone()
                        if result: episode_id = result[0]
                    
                    if not episode_id: continue

                    for server in episode.get("servers", []):
                        cursor.execute("INSERT INTO servers (episode_id, server_number, embed_url) VALUES (?, ?, ?)", 
                                       (episode_id, server.get("server_number"), server.get("embed_url")))
            conn.commit()
        except Exception as e:
            log_message(f"DB Season/Episode Error: {e}", level="error")
        finally:
            conn.close()

    def insert_movie_servers(self, show_id: int, servers: List[Dict]):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT OR IGNORE INTO seasons (show_id, season_number) VALUES (?, 1)", (show_id,))
            season_id = cursor.lastrowid
            if season_id == 0:
                cursor.execute("SELECT id FROM seasons WHERE show_id = ? AND season_number = 1", (show_id,))
                result = cursor.fetchone()
                if result: season_id = result[0]

            if not season_id: return

            cursor.execute("INSERT OR IGNORE INTO episodes (season_id, episode_number) VALUES (?, 1)", (season_id,))
            episode_id = cursor.lastrowid
            if episode_id == 0:
                cursor.execute("SELECT id FROM episodes WHERE season_id = ? AND episode_number = 1", (season_id,))
                result = cursor.fetchone()
                if result: episode_id = result[0]

            if not episode_id: return

            for server in servers:
                cursor.execute("INSERT INTO servers (episode_id, server_number, embed_url) VALUES (?, ?, ?)", 
                               (episode_id, server.get("server_number"), server.get("embed_url")))
            conn.commit()
        except Exception as e:
            log_message(f"DB Movie Server Error: {e}", level="error")
        finally:
            conn.close()

    def mark_progress(self, url: str, status: str, show_id: Optional[int] = None, error: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
            INSERT OR REPLACE INTO scrape_progress (url, status, show_id, error_message, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (url, status, show_id, error))
            conn.commit()
        finally:
            conn.close()

    def get_all_urls_from_files(self, json_files: List[str]) -> Dict[str, List[str]]:
        all_urls_map = {}
        total_count = 0
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                file_urls = data if isinstance(data, list) else data.get("urls", [])
                if isinstance(data, dict) and "series_animes" in data:
                    file_urls = data["series_animes"]
                
                all_urls_map[file_path] = file_urls
                total_count += len(file_urls)
            except FileNotFoundError:
                log_message(f"Warning: JSON file not found: {file_path}", level="warning")
                all_urls_map[file_path] = []
            except Exception as e:
                log_message(f"Error reading {file_path}: {e}", level="error")
                all_urls_map[file_path] = []
        
        GLOBAL_STATE['stats']['total_sources'] = total_count
        return all_urls_map

    def get_pending_urls(self, all_urls_map: Dict[str, List[str]]) -> Dict[str, List[str]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT url FROM scrape_progress WHERE status = 'completed'")
            completed = {row[0] for row in cursor.fetchall()}
        except sqlite3.Error as e:
            log_message(f"DB Error getting completed URLs: {e}", level="error")
            completed = set()
        finally:
            conn.close()
        
        pending_urls_map = {}
        total_pending = 0
        for file_path, urls in all_urls_map.items():
            pending_list = [url for url in urls if url not in completed]
            pending_urls_map[file_path] = pending_list
            total_pending += len(pending_list)
        
        GLOBAL_STATE['stats']['total_pending'] = total_pending
        return pending_urls_map

    def init_progress(self, all_urls: List[str]):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            urls_to_insert = [(url,) for url in all_urls]
            cursor.executemany("INSERT OR IGNORE INTO scrape_progress (url) VALUES (?)", urls_to_insert)
            conn.commit()
        except Exception as e:
            log_message(f"DB Error initializing progress: {e}", level="error")
        finally:
            conn.close()

    def get_shows_paginated(self, page: int = 1, per_page: int = 10, show_type: str = None):
        """Get shows with pagination"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            offset = (page - 1) * per_page
            
            if show_type:
                cursor.execute("SELECT COUNT(*) as count FROM shows WHERE type = ?", (show_type,))
            else:
                cursor.execute("SELECT COUNT(*) as count FROM shows")
            total = cursor.fetchone()['count']
            
            if show_type:
                cursor.execute("""
                    SELECT id, title, type, poster, imdb_rating, year, source_url, created_at 
                    FROM shows WHERE type = ? ORDER BY created_at DESC LIMIT ? OFFSET ?
                """, (show_type, per_page, offset))
            else:
                cursor.execute("""
                    SELECT id, title, type, poster, imdb_rating, year, source_url, created_at 
                    FROM shows ORDER BY created_at DESC LIMIT ? OFFSET ?
                """, (per_page, offset))
            
            shows = [dict(row) for row in cursor.fetchall()]
            return shows, total
        finally:
            conn.close()

# --- Scraper Logic ---

REGEX_PATTERNS = {
    'number': re.compile(r'(\d+)'),
    'movie': re.compile(r'(\/فيلم-|\/film-|\/movie-|%d9%81%d9%8a%d9%84%d9%85)', re.IGNORECASE),
    'episode': re.compile(r'(?:الحلقة|Episode)\s*(\d+)'),
    'watch_suffix': re.compile(r'/watch/?$'),
    'episode_id': re.compile(r'"id"\s*:\s*"(\d+)"'),
    'title_clean_prefix': re.compile(r'^\s*(فيلم|انمي|مسلسل|anime|film|movie|series)\s+', re.IGNORECASE | re.UNICODE),
    'title_clean_suffix': re.compile(r'\s+(مترجم|اون\s*لاين|اونلاين|online|مترجمة|مدبلج|مدبلجة)(\s+|$)', re.IGNORECASE | re.UNICODE)
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://web7.topcinema.cam/",
}

REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.3
VERIFY_SSL = False

SESSION = requests.Session()
retry_strategy = requests.packages.urllib3.util.retry.Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "POST"]
)
adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=retry_strategy)
SESSION.mount('https://', adapter)
SESSION.mount('http://', adapter)
if not VERIFY_SSL:
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

ARABIC_ORDINALS = {
    "الاول": 1, "الأول": 1, "الثاني": 2, "ثاني": 2, "الثالث": 3, "ثالث": 3,
    "الرابع": 4, "رابع": 4, "الخامس": 5, "خامس": 5, "السادس": 6, "سادس": 6,
    "السابع": 7, "سابع": 7, "الثامن": 8, "ثامن": 8, "التاسع": 9, "تاسع": 9,
    "العاشر": 10, "عاشر": 10,
}

def log_message(msg: str, level: str = "info") -> None:
    timestamp = time.strftime("%H:%M:%S")
    level_map = {"info": "INFO", "success": "SUCCESS", "warning": "WARN", "error": "ERROR"}
    formatted_msg = f"[{timestamp}] [{level_map.get(level, 'INFO')}] {msg}"
    GLOBAL_STATE['log_message'] = formatted_msg
    
    GLOBAL_STATE['log_buffer'].append(formatted_msg)
    if len(GLOBAL_STATE['log_buffer']) > 100:
        GLOBAL_STATE['log_buffer'].pop(0)

def fetch_html(url: str) -> Optional[BeautifulSoup]:
    if GLOBAL_STATE['stop_scraper']: return None
    if not url.startswith(('http://', 'https://')):
        log_message(f"Invalid URL scheme: {url}", level="error")
        return None
    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log_message(f"Request failed for {url}: {str(e)[:50]}", level="error")
    return None

def extract_number_from_text(text: str) -> Optional[int]:
    if not text: return None
    m = REGEX_PATTERNS['number'].search(text)
    if m: return int(m.group(1))
    lower = text.replace("ي", "ى").replace("أ", "ا").replace("إ", "ا").strip()
    for word, num in ARABIC_ORDINALS.items():
        if word in lower: return num
    return None

def clean_title(title: str) -> str:
    if not title: return title
    cleaned = REGEX_PATTERNS['title_clean_prefix'].sub('', title)
    prev = ""
    while prev != cleaned:
        prev = cleaned
        cleaned = REGEX_PATTERNS['title_clean_suffix'].sub(' ', cleaned)
    cleaned = ' '.join(cleaned.split()).strip(' -–—|:،؛')
    return cleaned

def get_trailer_embed_url(page_url: str, form_url: str) -> Optional[str]:
    """Improved trailer fetching with better error handling"""
    if GLOBAL_STATE['stop_scraper']: return None
    try:
        p = urlparse(page_url)
        base = f"{p.scheme}://{p.netloc}"
        trailer_endpoint = base + "/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
        encoded_form_url = quote(form_url, safe=':/')
        data_str = f"href={encoded_form_url}"
        
        trailer_headers = {
            "accept": "*/*", "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest", "referer": quote(page_url, safe=':/')
        }
        
        resp = SESSION.post(trailer_endpoint, headers=trailer_headers, data=data_str.encode('utf-8'),
                          timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, "html.parser")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            trailer_url = iframe["src"].strip()
            if trailer_url and trailer_url.startswith(('http://', 'https://')):
                return trailer_url
        return None
    except Exception as e:
        log_message(f"Trailer fetch error: {str(e)[:50]}", level="warning")
        return None

def get_episode_servers(episode_id: str, referer: Optional[str] = None, total_servers: int = 10) -> List[Dict]:
    if GLOBAL_STATE['stop_scraper']: return []
    servers: List[Dict] = []
    base = "https://web7.topcinema.cam"
    if referer:
        try:
            p = urlparse(referer)
            base = f"{p.scheme}://{p.netloc}"
        except Exception:
            pass
    
    server_url = base + "/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    server_headers = {
        "accept": "*/*", "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
    }
    if referer:
        server_headers["Referer"] = referer

    def fetch_one(i: int):
        if GLOBAL_STATE['stop_scraper']: return None
        try:
            data = {"id": str(episode_id), "i": str(i)}
            resp = SESSION.post(server_url, headers=server_headers, data=data, timeout=5, verify=VERIFY_SSL)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            iframe = soup.find("iframe")
            if iframe and iframe.get("src") and iframe.get("src").strip():
                return {"server_number": i, "embed_url": iframe.get("src").strip()}
            return None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=min(12, total_servers)) as ex:
        futures = {ex.submit(fetch_one, i): i for i in range(total_servers)}
        for fut in as_completed(futures):
            if GLOBAL_STATE['stop_scraper']:
                ex.shutdown(wait=False, cancel_futures=True)
                break
            res = fut.result()
            if res:
                servers.append(res)

    servers.sort(key=lambda x: x.get("server_number", 0))
    return servers

def extract_episode_id_from_watch_page(soup: BeautifulSoup) -> Optional[str]:
    if not soup: return None
    li = soup.select_one(".watch--servers--list li.server--item[data-id]")
    if li and li.has_attr("data-id"):
        return li["data-id"].strip()
    for script in soup.find_all("script"):
        if script.string:
            m = REGEX_PATTERNS['episode_id'].search(script.string)
            if m: return m.group(1)
    return None

def scrape_season_episodes(season_url: str) -> List[Dict]:
    if GLOBAL_STATE['stop_scraper']: return []
    list_url = season_url.rstrip('/') + '/list/' if not season_url.endswith('/list/') else season_url
    
    soup = fetch_html(list_url)
    if not soup: return []

    episodes: List[Dict] = []
    seen = set()
    anchors = soup.select('.allepcont .row > a')
    if not anchors:
        anchors = [x for x in soup.find_all('a') if (x.find(class_='epnum') or (x.get('title') and 'الحلقة' in x.get('title')))]

    def process_episode(a):
        if GLOBAL_STATE['stop_scraper']: return None
        try:
            raw_href = a.get('href')
            if not raw_href: return None
            
            ep_title = a.get('title', '')
            ep_num_text = a.get_text(" ", strip=True)
            
            ep_num = None
            m_num = REGEX_PATTERNS['episode'].search(ep_title) or REGEX_PATTERNS['episode'].search(ep_num_text)
            if m_num:
                ep_num = int(m_num.group(1))
            
            if not ep_num:
                ep_num = extract_number_from_text(ep_title) or extract_number_from_text(ep_num_text)

            if not ep_num or ep_num == 0: ep_num = 999
            
            key = (str(ep_num) or ep_title or raw_href or '').strip()
            if key in seen: return None
            seen.add(key)
            
            watch_url = raw_href.rstrip('/') + '/watch/'
            ep_watch_soup = fetch_html(watch_url)
            episode_id = extract_episode_id_from_watch_page(ep_watch_soup) if ep_watch_soup else None
            
            server_list: List[Dict] = []
            if episode_id:
                server_list = get_episode_servers(episode_id, referer=watch_url, total_servers=10)
            
            return {"episode_number": ep_num, "servers": server_list}
        except Exception as e:
            log_message(f"Error processing episode {a.get('href')}: {e}", level="error")
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(process_episode, a) for a in anchors]
        for fut in as_completed(futures):
            if GLOBAL_STATE['stop_scraper']:
                ex.shutdown(wait=False, cancel_futures=True)
                break
            res = fut.result()
            if res:
                episodes.append(res)

    episodes.sort(key=lambda e: e.get("episode_number", 999))
    episodes = [e for e in episodes if e.get("episode_number", 999) != 999]
    return episodes

def extract_media_details(soup: BeautifulSoup) -> Dict:
    details = {
        "title": "Unknown", "poster": None, "synopsis": "",
        "imdb_rating": None, "metadata": {}
    }
    try:
        title_el = soup.find("h1", class_="post-title")
        if title_el:
            details["title"] = clean_title(title_el.get_text(strip=True))
        
        poster_wrap = soup.find('div', class_='image')
        if poster_wrap:
            img_tag = poster_wrap.find('img')
            if img_tag: details["poster"] = img_tag.get('src') or img_tag.get('data-src')
        
        story = soup.find('div', class_='story')
        if story:
            p = story.find('p')
            if p: details["synopsis"] = p.get_text(strip=True)
        
        imdb_box = soup.select_one(".UnderPoster .imdbR")
        if imdb_box:
            sp = imdb_box.find("span")
            if sp:
                try: details["imdb_rating"] = float(sp.get_text(strip=True))
                except ValueError: pass
        
        tax = soup.find('ul', class_='RightTaxContent')
        if tax:
            for li in tax.find_all('li'):
                key_el = li.find('span')
                if key_el:
                    key = key_el.get_text(strip=True).replace(':', '')
                    links = [a.get_text(strip=True) for a in li.find_all('a') if a.get_text(strip=True)]
                    details["metadata"][key] = links if links else li.find('strong').get_text(strip=True) if li.find('strong') else ""
    except Exception as e:
        log_message(f"Error extracting details: {e}", level="error")
    
    key_mapping = {
        "قسم المسلسل": "category", "قسم الفيلم": "category", "نوع المسلسل": "genres",
        "نوع الفيلم": "genres", "النوع": "genres", "جودة المسلسل": "quality",
        "جودة الفيلم": "quality", "عدد الحلقات": "episode_count", "توقيت المسلسل": "duration",
        "توقيت الفيلم": "duration", "مدة الفيلم": "duration", "موعد الصدور": "release_year",
        "سنة الانتاج": "release_year", "لغة المسلسل": "language", "لغة الفيلم": "language",
        "دولة المسلسل": "country", "دولة الفيلم": "country", "المخرجين": "directors",
        "المخرج": "directors", "بطولة": "cast"
    }
    
    mapped_metadata = {}
    for k, v in details["metadata"].items():
        clean_key = k.strip().rstrip(':')
        new_key = key_mapping.get(clean_key, clean_key)
        if new_key in key_mapping.values():
            mapped_metadata[new_key] = v
    details["metadata"] = mapped_metadata
    return details

def scrape_series(url: str) -> Optional[Dict]:
    if GLOBAL_STATE['stop_scraper']: return None
    soup = fetch_html(url)
    if not soup: return None
    
    details = extract_media_details(soup)
    seasons: List[Dict] = []
    season_urls: Dict[int, str] = {}
    seen_urls = set()
    
    for s_el in soup.select('div.Small--Box.Season'):
        a_el = s_el.find('a')
        if not a_el or not a_el.get('href'): continue
        s_url = a_el.get('href')
        if s_url in seen_urls: continue
        seen_urls.add(s_url)
        s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
        s_num = extract_number_from_text(s_title) or 1
        
        s_poster = None
        img = a_el.find('img')
        if img: s_poster = img.get('src') or img.get('data-src')

        season_urls[s_num] = s_url
        seasons.append({"season_number": s_num, "poster": s_poster, "episodes": []})

    if not seasons:
        for a_el in soup.find_all('a', href=True):
            href = a_el['href']
            if '/series/' in href and ('الموسم' in href or 'season' in a_el.get_text(strip=True, default='').lower()):
                if href in seen_urls: continue
                seen_urls.add(href)
                s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
                s_num = extract_number_from_text(s_title) or extract_number_from_text(href) or 1
                season_urls[s_num] = href
                seasons.append({"season_number": s_num, "poster": None, "episodes": []})

    seasons.sort(key=lambda s: s.get('season_number', 0))

    if not seasons:
        season_urls[1] = url
        seasons.append({"season_number": 1, "poster": details["poster"], "episodes": []})

    for season in seasons:
        if GLOBAL_STATE['stop_scraper']: break
        s_num = season["season_number"]
        if s_num in season_urls:
            season["episodes"] = scrape_season_episodes(season_urls[s_num])

    trailer_url = None
    episode_page_url = None
    
    # Get first episode's actual page URL from the season HTML
    if season_urls:
        first_season_url = list(season_urls.values())[0]
        temp_soup = fetch_html(first_season_url)
        if temp_soup:
            first_ep_link = temp_soup.select_one(".allepcont .row > a")
            if first_ep_link:
                episode_page_url = first_ep_link.get("href")
    
    # Use episode page URL for trailer if found
    if episode_page_url:
        trailer_url = get_trailer_embed_url(url, episode_page_url)
    
    # Fallback to main URL if episode trailer didn't work
    if not trailer_url:
        trailer_url = get_trailer_embed_url(url, url)

    return {
        "title": details["title"], "type": "series", "imdb_rating": details["imdb_rating"],
        "poster": details["poster"], "synopsis": details["synopsis"], "metadata": details["metadata"],
        "trailer": trailer_url, "source_url": url, "seasons": seasons
    }

def scrape_movie(url: str) -> Optional[Dict]:
    if GLOBAL_STATE['stop_scraper']: return None
    
    details_soup = fetch_html(url)
    if not details_soup: return None
    
    details = extract_media_details(details_soup)
    
    watch_url = url.rstrip('/') + '/watch/'
    watch_soup = fetch_html(watch_url)
    if not watch_soup: return None
        
    episode_id = extract_episode_id_from_watch_page(watch_soup)
    servers = get_episode_servers(episode_id, referer=watch_url) if episode_id else []
    trailer_url = get_trailer_embed_url(url, url)

    return {
        "title": details["title"], "type": "movie",
        "year": extract_number_from_text(details["title"]),
        "imdb_rating": details["imdb_rating"], "poster": details["poster"],
        "synopsis": details["synopsis"], "metadata": details["metadata"],
        "trailer": trailer_url, "source_url": url,
        "streaming_servers": servers, "scraped_at": datetime.now().isoformat()
    }

def run_single(url_input: str, force_type: Optional[str] = None) -> Optional[Dict]:
    """Properly handle anime/series/movie type detection based on URL pattern"""
    if GLOBAL_STATE['stop_scraper']: return None
    url = url_input.strip()
    
    result: Optional[Dict] = None
    
    if "فيلم" in url or REGEX_PATTERNS['movie'].search(url):
        result = scrape_movie(url)
        if result:
            result['type'] = 'movie'
    elif "انمي" in url:
        result = scrape_series(url)
        if result:
            result['type'] = 'anime'
    elif "مسلسل" in url:
        result = scrape_series(url)
        if result:
            result['type'] = 'series'
    else:
        # Fallback: try to determine by scraping
        result = scrape_series(url)
        if result:
            result['type'] = 'series'
    
    return result

# --- Scraper Control Thread ---

def run_scraper_task():
    try:
        db = Database(GLOBAL_STATE['db_path'])
        
        all_urls_map = db.get_all_urls_from_files(GLOBAL_STATE['json_files'])
        all_urls_list = [url for urls in all_urls_map.values() for url in urls]
        
        pending_urls_map = db.get_pending_urls(all_urls_map)
        
        db.init_progress(all_urls_list)
        
        log_message(f"Found {GLOBAL_STATE['stats']['total_pending']} pending items out of {GLOBAL_STATE['stats']['total_sources']} total.", level="info")
        
        start_time = time.time()
        
        for json_file, urls in pending_urls_map.items():
            if GLOBAL_STATE['stop_scraper']:
                log_message("Scraper stop signal received.", level="warning")
                break
            
            if not urls:
                log_message(f"No pending URLs in {json_file}, skipping.", level="info")
                continue

            GLOBAL_STATE['stats']['current_file'] = json_file
            log_message(f"Processing {len(urls)} items from {json_file}...", level="info")
            
            
            for idx, url in enumerate(urls, 1):
                if GLOBAL_STATE['stop_scraper']:
                    log_message("Scraper stop signal received.", level="warning")
                    break
                
                log_message(f"[{idx}/{len(urls)}] Scraping: {url}", level="info")
                
                try:
                    result = run_single(url)
                    if result:
                        show_id = db.insert_show(result)
                        if show_id:
                            if result.get("type") in ["series", "anime"]:
                                db.insert_seasons_and_episodes(show_id, result.get("seasons", []))
                                if result.get("type") == "anime":
                                    GLOBAL_STATE['stats']['anime'] += 1
                                else:
                                    GLOBAL_STATE['stats']['series'] += 1
                            else:
                                db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                                GLOBAL_STATE['stats']['movies'] += 1
                            
                            db.mark_progress(url, "completed", show_id)
                            GLOBAL_STATE['stats']['completed'] += 1
                        else:
                            db.mark_progress(url, "failed", error="Duplicate or DB error")
                            GLOBAL_STATE['stats']['failed'] += 1
                            GLOBAL_STATE['stats']['failed_urls'].append({"url": url, "error": "Duplicate or DB Error"})
                    else:
                        db.mark_progress(url, "failed", error="Scraping returned no data")
                        GLOBAL_STATE['stats']['failed'] += 1
                        GLOBAL_STATE['stats']['failed_urls'].append({"url": url, "error": "Scraping returned no data"})
                except Exception as e:
                    error_str = str(e)[:100]
                    db.mark_progress(url, "failed", error=error_str)
                    GLOBAL_STATE['stats']['failed'] += 1
                    GLOBAL_STATE['stats']['failed_urls'].append({"url": url, "error": error_str})
                    log_message(f"Error scraping {url}: {error_str}", level="error")
        
        elapsed = time.time() - start_time
        time_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        log_message(f"Scraping finished in {time_str}. Completed: {GLOBAL_STATE['stats']['completed']}, Failed: {GLOBAL_STATE['stats']['failed']}", level="success")

    except Exception as e:
        log_message(f"Fatal scraper error: {e}", level="error")
    finally:
        GLOBAL_STATE['scraper_running'] = False
        GLOBAL_STATE['stop_scraper'] = False
        GLOBAL_STATE['stats']['current_file'] = "N/A"

def run_test_script_task():
    """Run test script with 10 parallel fetchers and sequential writer"""
    try:
        import random
        
        db = Database(GLOBAL_STATE['db_path'])
        db.init_database()
        
        # Load URLs from JSON files
        anime_urls = []
        series_urls = []
        movie_urls = []
        
        for json_file in GLOBAL_STATE['json_files']:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    urls = [item['url'] for item in data if 'url' in item]
                    
                    if "series_animes" in json_file:
                        for url in urls:
                            if "انمي" in url:
                                anime_urls.append(url)
                            elif "مسلسل" in url:
                                series_urls.append(url)
                            elif "فيلم" in url:
                                movie_urls.append(url)
                    elif "movies" in json_file:
                        movie_urls.extend(urls)
            except Exception as e:
                log_message(f"Error loading JSON file {json_file}: {e}", level="error")
        
        selected_urls = []
        if anime_urls:
            selected_urls.extend(random.sample(anime_urls, min(3, len(anime_urls))))
            log_message(f"Selected {min(3, len(anime_urls))} anime URLs for testing", level="info")
        else:
            log_message("No anime URLs found in JSON files", level="warning")
        
        if series_urls:
            selected_urls.extend(random.sample(series_urls, min(3, len(series_urls))))
            log_message(f"Selected {min(3, len(series_urls))} series URLs for testing", level="info")
        else:
            log_message("No series URLs found in JSON files", level="warning")
        
        if movie_urls:
            selected_urls.extend(random.sample(movie_urls, min(3, len(movie_urls))))
            log_message(f"Selected {min(3, len(movie_urls))} movie URLs for testing", level="info")
        else:
            log_message("No movie URLs found in JSON files", level="warning")
        
        if not selected_urls:
            log_message("No URLs selected for testing. Check JSON files.", level="error")
            return
        
        log_message(f"Starting test script with {len(selected_urls)} URLs (10 parallel fetchers + sequential writer)", level="info")
        
        GLOBAL_STATE['test_stats'] = {
            "fetching": 0,
            "in_queue": 0,
            "written": 0,
            "failed": 0
        }
        
        fetch_queue = Queue()
        fetching_lock = threading.Lock()
        
        def fetcher_worker(url_list, worker_id):
            """Fetch shows and add to queue - truly parallel"""
            for url in url_list:
                if GLOBAL_STATE['stop_scraper']:
                    break
                
                with fetching_lock:
                    GLOBAL_STATE['test_stats']['fetching'] += 1
                
                log_message(f"[WORKER {worker_id}] Fetching: {url}", level="info")
                
                try:
                    result = run_single(url)
                    if result:
                        fetch_queue.put({"url": url, "result": result, "worker_id": worker_id})
                        with fetching_lock:
                            GLOBAL_STATE['test_stats']['fetching'] -= 1
                            GLOBAL_STATE['test_stats']['in_queue'] += 1
                        log_message(f"[WORKER {worker_id}] ✓ Queued: {result.get('title')}", level="success")
                    else:
                        with fetching_lock:
                            GLOBAL_STATE['test_stats']['fetching'] -= 1
                            GLOBAL_STATE['test_stats']['failed'] += 1
                        log_message(f"[WORKER {worker_id}] ✗ Failed: {url}", level="error")
                except Exception as e:
                    with fetching_lock:
                        GLOBAL_STATE['test_stats']['fetching'] -= 1
                        GLOBAL_STATE['test_stats']['failed'] += 1
                    log_message(f"[WORKER {worker_id}] ✗ Error: {str(e)[:60]}", level="error")
        
        def writer_worker():
            """Write shows from queue to database one by one - truly sequential"""
            while True:
                try:
                    item = fetch_queue.get(timeout=2)
                    if item is None:
                        break
                    
                    result = item['result']
                    worker_id = item['worker_id']
                    
                    log_message(f"[WRITER] Writing: {result.get('title')}", level="info")
                    
                    try:
                        show_id = db.insert_show(result)
                        if show_id:
                            if result.get("type") in ["series", "anime"]:
                                db.insert_seasons_and_episodes(show_id, result.get("seasons", []))
                            else:
                                db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                            
                            with fetching_lock:
                                GLOBAL_STATE['test_stats']['in_queue'] -= 1
                                GLOBAL_STATE['test_stats']['written'] += 1
                            log_message(f"[WRITER] ✓ Written: {result.get('title')}", level="success")
                        else:
                            with fetching_lock:
                                GLOBAL_STATE['test_stats']['in_queue'] -= 1
                                GLOBAL_STATE['test_stats']['failed'] += 1
                            log_message(f"[WRITER] ✗ DB insert failed", level="error")
                    except Exception as e:
                        with fetching_lock:
                            GLOBAL_STATE['test_stats']['in_queue'] -= 1
                            GLOBAL_STATE['test_stats']['failed'] += 1
                        log_message(f"[WRITER] ✗ Error: {str(e)[:60]}", level="error")
                    
                    fetch_queue.task_done()
                except queue.Empty:
                    if fetch_queue.empty():
                        break
                except Exception as e:
                    log_message(f"[WRITER] Fatal error: {str(e)[:60]}", level="error")
                    break
        
        fetcher_threads = []
        urls_per_worker = max(1, len(selected_urls) // 10)
        
        for worker_id in range(10):
            start_idx = worker_id * urls_per_worker
            end_idx = start_idx + urls_per_worker if worker_id < 9 else len(selected_urls)
            worker_urls = selected_urls[start_idx:end_idx]
            
            if worker_urls:
                t = threading.Thread(target=fetcher_worker, args=(worker_urls, worker_id + 1), daemon=False)
                t.start()
                fetcher_threads.append(t)
        
        # Start writer thread
        writer_thread = threading.Thread(target=writer_worker, daemon=False)
        writer_thread.start()
        
        # Wait for all fetchers to complete
        for t in fetcher_threads:
            t.join()
        
        log_message("All fetchers completed. Waiting for queue to empty...", level="info")
        
        # Wait for queue to be processed
        fetch_queue.join()
        
        # Signal writer to stop
        fetch_queue.put(None)
        writer_thread.join()
        
        log_message(f"Test completed! Written: {GLOBAL_STATE['test_stats']['written']}, Failed: {GLOBAL_STATE['test_stats']['failed']}", level="success")

    except Exception as e:
        log_message(f"Fatal test script error: {e}", level="error")
    finally:
        GLOBAL_STATE['scraper_running'] = False
        GLOBAL_STATE['stop_scraper'] = False

# --- Flask Web Server ---

app = Flask(__name__)

@app.route('/')
def index():
    """Main dashboard with enhanced stats and test script button"""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Scraper Dashboard</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                   background-color: #121212; color: #E0E0E0; margin: 0; padding: 20px; font-size: 16px; }
            .container { max-width: 1200px; margin: 0 auto; background-color: #1E1E1E; border: 1px solid #333;
                         border-radius: 8px; padding: 25px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
            h1 { color: #FFFFFF; border-bottom: 2px solid #444; padding-bottom: 10px; margin-top: 0; }
            h2 { color: #E0E0E0; border-bottom: 1px solid #333; padding-bottom: 8px; margin-top: 30px; }
            .controls { margin-bottom: 20px; display: flex; gap: 15px; flex-wrap: wrap; }
            button { background-color: #333; color: #E0E0E0; border: 1px solid #555; padding: 12px 20px;
                     border-radius: 5px; cursor: pointer; font-size: 16px; transition: all 0.2s ease; }
            button:hover { background-color: #444; border-color: #777; }
            button:disabled { background-color: #2a2a2a; color: #555; border-color: #444; cursor: not-allowed; }
            button.test-btn { background-color: #f39c12; }
            button.test-btn:hover { background-color: #e67e22; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; }
            .stat-box { background-color: #2a2a2a; border: 1px solid #333; border-radius: 5px; padding: 15px; }
            .stat-box strong { display: block; font-size: 24px; color: #FFFFFF; }
            .stat-box span { font-size: 14px; color: #AAA; }
            #log-container { background-color: #2a2a2a; border: 1px solid #333; padding: 15px; border-radius: 5px;
                            max-height: 400px; overflow-y: auto; font-family: "Courier New", Courier, monospace;
                            font-size: 13px; line-height: 1.5; }
            .log-line { color: #E0E0E0; padding: 2px 0; }
            .log-line.success { color: #28a745; }
            .log-line.error { color: #dc3545; }
            .log-line.warning { color: #ffc107; }
            .test-stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-top: 15px; }
            .test-stat-box { background-color: #2a2a2a; border: 1px solid #444; border-radius: 5px; padding: 10px; text-align: center; }
            .test-stat-box strong { display: block; font-size: 18px; color: #f39c12; }
            .test-stat-box span { font-size: 12px; color: #AAA; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>TopCinema Scraper Dashboard</h1>
            <nav>
                <a href="/" class="active">Dashboard</a> | <a href="/db-view">DB Viewer</a> | <a href="/stats">Stats</a>
            </nav>
            <div class="controls">
                <button id="start-btn">▶ Start Scraper</button>
                <button id="stop-btn" disabled>⏹ Stop Scraper</button>
                <button id="test-btn" class="test-btn">🧪 Test Script (3+3+3)</button>
            </div>
            
            <h2>Live Stats</h2>
            <p>Current File: <strong id="current-file">N/A</strong></p>
            <div class="stats-grid">
                <div class="stat-box"><strong id="pending">0</strong><span>Pending</span></div>
                <div class="stat-box"><strong id="completed">0</strong><span>Completed</span></div>
                <div class="stat-box"><strong id="failed">0</strong><span>Failed</span></div>
                <div class="stat-box"><strong id="anime">0</strong><span>Anime</span></div>
                <div class="stat-box"><strong id="series">0</strong><span>Series</span></div>
                <div class="stat-box"><strong id="movies">0</strong><span>Movies</span></div>
                <div class="stat-box"><strong id="total">0</strong><span>Total Sources</span></div>
            </div>
            
            <div class="progress-bar">
                <div class="progress-fill" id="progress-fill" style="width: 0%">0%</div>
            </div>

            <h2>Test Script Status</h2>
            <div class="test-stats-grid">
                <div class="test-stat-box"><strong id="test-fetching">0</strong><span>Fetching</span></div>
                <div class="test-stat-box"><strong id="test-queue">0</strong><span>In Queue</span></div>
                <div class="test-stat-box"><strong id="test-written">0</strong><span>Written</span></div>
                <div class="test-stat-box"><strong id="test-failed">0</strong><span>Failed</span></div>
            </div>

            <h2>Live Log</h2>
            <div id="log-container"></div>

            <h2>Failed URLs</h2>
            <div id="failed-urls"><p>No failed URLs yet.</p></div>
        </div>

        <script>
            const startBtn = document.getElementById('start-btn');
            const stopBtn = document.getElementById('stop-btn');
            const testBtn = document.getElementById('test-btn');
            const logContainer = document.getElementById('log-container');
            const pending = document.getElementById('pending');
            const completed = document.getElementById('completed');
            const failed = document.getElementById('failed');
            const anime = document.getElementById('anime');
            const series = document.getElementById('series');
            const movies = document.getElementById('movies');
            const total = document.getElementById('total');
            const currentFile = document.getElementById('current-file');
            const failedUrlsDiv = document.getElementById('failed-urls');
            const progressFill = document.getElementById('progress-fill');
            
            const testFetching = document.getElementById('test-fetching');
            const testQueue = document.getElementById('test-queue');
            const testWritten = document.getElementById('test-written');
            const testFailed = document.getElementById('test-failed');

            async function fetchStatus() {
                try {
                    const response = await fetch('/api/status');
                    const data = await response.json();
                    
                    startBtn.disabled = data.scraper_running;
                    stopBtn.disabled = !data.scraper_running;
                    testBtn.disabled = data.scraper_running;

                    const logs = data.log_buffer || [];
                    logContainer.innerHTML = logs.map(log => {
                        let className = 'log-line';
                        if (log.includes('[SUCCESS]')) className += ' success';
                        else if (log.includes('[ERROR]')) className += ' error';
                        else if (log.includes('[WARN]')) className += ' warning';
                        return `<div class="${className}">${log}</div>`;
                    }).join('');
                    logContainer.scrollTop = logContainer.scrollHeight;

                    const stats = data.stats;
                    const remaining = stats.total_pending - stats.completed - stats.failed;
                    pending.textContent = remaining > 0 ? remaining : 0;
                    completed.textContent = stats.completed;
                    failed.textContent = stats.failed;
                    anime.textContent = stats.anime;
                    series.textContent = stats.series;
                    movies.textContent = stats.movies;
                    total.textContent = stats.total_sources;
                    currentFile.textContent = stats.current_file;

                    if (stats.total_sources > 0) {
                        const progress = ((stats.completed + stats.failed) / stats.total_sources) * 100;
                        progressFill.style.width = progress + '%';
                        progressFill.textContent = Math.round(progress) + '%';
                    }

                    if (stats.failed_urls.length > 0) {
                        failedUrlsDiv.innerHTML = stats.failed_urls.map(item => 
                            `<div><code>${item.url}</code><span>Error: ${item.error}</span></div>`
                        ).join('');
                    } else {
                        failedUrlsDiv.innerHTML = '<p>No failed URLs yet.</p>';
                    }
                    
                    const testStats = data.test_stats || {};
                    testFetching.textContent = testStats.fetching || 0;
                    testQueue.textContent = testStats.in_queue || 0;
                    testWritten.textContent = testStats.written || 0;
                    testFailed.textContent = testStats.failed || 0;
                } catch (e) {
                    logContainer.innerHTML = '<div class="log-line error">Error fetching status</div>';
                }
            }

            startBtn.addEventListener('click', async () => {
                try {
                    await fetch('/api/start', { method: 'POST' });
                    fetchStatus();
                } catch (e) {
                    logContainer.innerHTML = '<div class="log-line error">Error starting scraper</div>';
                }
            });

            stopBtn.addEventListener('click', async () => {
                try {
                    await fetch('/api/stop', { method: 'POST' });
                    fetchStatus();
                } catch (e) {
                    logContainer.innerHTML = '<div class="log-line error">Error stopping scraper</div>';
                }
            });

            testBtn.addEventListener('click', async () => {
                try {
                    await fetch('/api/test', { method: 'POST' });
                    fetchStatus();
                } catch (e) {
                    logContainer.innerHTML = '<div class="log-line error">Error starting test script</div>';
                }
            });

            setInterval(fetchStatus, 1000);
            fetchStatus();
        </script>
    </body>
    </html>
    """
    return Response(html, mimetype='text/html')

@app.route('/api/test', methods=['POST'])
def api_test():
    global SCRAPER_THREAD
    if not GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE['scraper_running'] = True
        GLOBAL_STATE['stop_scraper'] = False
        
        log_message("Test script starting...", level="info")
        
        SCRAPER_THREAD = threading.Thread(target=run_test_script_task, daemon=True)
        SCRAPER_THREAD.start()
        return jsonify({"success": True, "message": "Test script started."})
    return jsonify({"success": False, "message": "Scraper already running."})

@app.route('/db-view')
def db_view():
    """Enhanced database viewer showing actual data with pagination (50 per page)"""
    db_path = GLOBAL_STATE['db_path']
    if not os.path.exists(db_path):
        return "Database file not found. Run the scraper to create it.", 404

    page = request.args.get('page', 1, type=int)
    show_type = request.args.get('type', None, type=str)
    per_page = 50

    try:
        db = Database(db_path)
        shows, total = db.get_shows_paginated(page, per_page, show_type)
        total_pages = (total + per_page - 1) // per_page
        
        rows_html = ""
        for show in shows:
            rows_html += f"""
            <tr>
                <td>{show['id']}</td>
                <td>{show['title']}</td>
                <td><span style="background-color: {'#28a745' if show['type'] == 'anime' else '#3498db' if show['type'] == 'series' else '#e74c3c'}; padding: 3px 8px; border-radius: 3px; color: white; font-size: 12px;">{show['type'].upper()}</span></td>
                <td>{show['imdb_rating'] or 'N/A'}</td>
                <td>{show['year'] or 'N/A'}</td>
                <td><a href="{show['source_url']}" target="_blank" style="color: #3498db;">View</a></td>
            </tr>
            """
        
        pagination_html = ""
        if total_pages > 1:
            pagination_html = '<div style="margin-top: 20px; text-align: center;">'
            # Show first, previous, next, last
            if page > 1:
                type_param = f"&type={show_type}" if show_type else ""
                pagination_html += f'<a href="/db-view?page=1{type_param}" style="color: #3498db; padding: 5px 10px; margin: 0 2px; border-radius: 4px;">« First</a>'
                pagination_html += f'<a href="/db-view?page={page-1}{type_param}" style="color: #3498db; padding: 5px 10px; margin: 0 2px; border-radius: 4px;">‹ Prev</a>'
            
            # Show page numbers (max 10 visible)
            start_page = max(1, page - 5)
            end_page = min(total_pages, page + 5)
            
            for p in range(start_page, end_page + 1):
                active = "style='font-weight: bold; background-color: #2a2a2a;'" if p == page else ""
                type_param = f"&type={show_type}" if show_type else ""
                pagination_html += f'<a href="/db-view?page={p}{type_param}" {active} style="color: #3498db; padding: 5px 10px; margin: 0 2px; border-radius: 4px;">{p}</a>'
            
            if page < total_pages:
                type_param = f"&type={show_type}" if show_type else ""
                pagination_html += f'<a href="/db-view?page={page+1}{type_param}" style="color: #3498db; padding: 5px 10px; margin: 0 2px; border-radius: 4px;">Next ›</a>'
                pagination_html += f'<a href="/db-view?page={total_pages}{type_param}" style="color: #3498db; padding: 5px 10px; margin: 0 2px; border-radius: 4px;">Last »</a>'
            
            pagination_html += '</div>'

        type_filter = ""
        if show_type:
            type_filter = f'<p>Filtering by type: <strong>{show_type.upper()}</strong> | <a href="/db-view" style="color: #3498db;">Clear Filter</a></p>'
        else:
            type_filter = '<p>Filter by type: <a href="/db-view?type=anime" style="color: #3498db;">Anime</a> | <a href="/db-view?type=series" style="color: #3498db;">Series</a> | <a href="/db-view?type=movie" style="color: #3498db;">Movies</a></p>'

        html = f"""
        <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>DB Viewer</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                   background-color: #121212; color: #E0E0E0; margin: 0; padding: 20px; }}
            .container {{ max-width: 1400px; margin: 0 auto; background-color: #1E1E1E; border: 1px solid #333;
                         border-radius: 8px; padding: 25px; }}
            h1 {{ color: #FFFFFF; border-bottom: 2px solid #444; padding-bottom: 10px; margin-top: 0; }}
            h2 {{ color: #E0E0E0; border-bottom: 1px solid #333; padding-bottom: 8px; margin-top: 30px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 14px; }}
            th, td {{ border: 1px solid #333; padding: 12px; text-align: left; }}
            th {{ background-color: #2a2a2a; color: #FFFFFF; }}
            tr:nth-child(even) {{ background-color: #2a2a2a; }}
            tr:hover {{ background-color: #333; }}
            nav {{ margin-bottom: 20px; }}
            nav a {{ color: #3498db; text-decoration: none; padding: 5px 10px; border-radius: 4px; }}
            nav a:hover {{ background-color: #333; }}
            nav a.active {{ font-weight: bold; background-color: #2a2a2a; }}
            .download-btn {{ background-color: #28a745; color: white; padding: 10px 20px; border-radius: 5px; text-decoration: none; display: inline-block; margin-bottom: 20px; }}
            .download-btn:hover {{ background-color: #218838; }}
            .info {{ color: #AAA; font-size: 14px; margin-top: 10px; }}
        </style>
        </head><body><div class="container">
        <h1>Database Viewer</h1>
        <nav>
            <a href="/">Dashboard</a> | <a href="/db-view" class="active">DB Viewer</a> | <a href="/stats">Stats</a>
        </nav>
        <a href="/download-db" class="download-btn">📥 Download Database</a>
        <h2>Shows Data (Total: {total}, Page {page} of {total_pages})</h2>
        {type_filter}
        <table>
            <tr><th>ID</th><th>Title</th><th>Type</th><th>Rating</th><th>Year</th><th>Link</th></tr>
            {rows_html}
        </table>
        {pagination_html}
        <div class="info">Showing {len(shows)} of {total} records | {per_page} per page</div>
        </div></body></html>
        """
        return Response(html, mimetype='text/html')
    except Exception as e:
        return f"<h2>Error loading database</h2><p>{e}</p>", 500

@app.route('/stats')
def stats_page():
    """Enhanced stats page with detailed metrics"""
    db_path = GLOBAL_STATE['db_path']
    if not os.path.exists(db_path):
        return "Database file not found.", 404

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get counts by type
        cursor.execute("SELECT type, COUNT(*) as count FROM shows GROUP BY type")
        type_counts = {row['type']: row['count'] for row in cursor.fetchall()}
        
        # Get total episodes
        cursor.execute("SELECT COUNT(*) as count FROM episodes")
        total_episodes = cursor.fetchone()['count']
        
        # Get total servers
        cursor.execute("SELECT COUNT(*) as count FROM servers")
        total_servers = cursor.fetchone()['count']
        
        # Get average rating
        cursor.execute("SELECT AVG(imdb_rating) as avg_rating FROM shows WHERE imdb_rating IS NOT NULL")
        avg_rating = cursor.fetchone()['avg_rating'] or 0
        
        # Get shows with trailers
        cursor.execute("SELECT COUNT(*) as count FROM shows WHERE trailer IS NOT NULL")
        shows_with_trailers = cursor.fetchone()['count']
        
        # Get total shows
        cursor.execute("SELECT COUNT(*) as count FROM shows")
        total_shows = cursor.fetchone()['count']
        
        conn.close()
        
        anime_count = type_counts.get('anime', 0)
        series_count = type_counts.get('series', 0)
        movie_count = type_counts.get('movie', 0)
        
        html = f"""
        <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Stats</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                   background-color: #121212; color: #E0E0E0; margin: 0; padding: 20px; }}
            .container {{ max-width: 1200px; margin: 0 auto; background-color: #1E1E1E; border: 1px solid #333;
                         border-radius: 8px; padding: 20px; }}
            h1 {{ color: #FFFFFF; border-bottom: 2px solid #444; padding-bottom: 10px; margin-top: 0; }}
            h2 {{ color: #E0E0E0; border-bottom: 1px solid #333; padding-bottom: 8px; margin-top: 30px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 20px; }}
            .stat-card {{ background-color: #2a2a2a; border: 1px solid #333; border-radius: 8px; padding: 20px; }}
            .stat-card h3 {{ margin: 0 0 10px 0; color: #FFFFFF; font-size: 14px; text-transform: uppercase; }}
            .stat-card .value {{ font-size: 32px; font-weight: bold; color: #28a745; }}
            nav {{ margin-bottom: 20px; }}
            nav a {{ color: #3498db; text-decoration: none; padding: 5px 10px; border-radius: 4px; }}
            nav a:hover {{ background-color: #333; }}
            nav a.active {{ font-weight: bold; background-color: #2a2a2a; }}
        </style>
        </head><body><div class="container">
        <h1>Database Statistics</h1>
        <nav>
            <a href="/">Dashboard</a> | <a href="/db-view">DB Viewer</a> | <a href="/stats" class="active">Stats</a>
        </nav>
        <div class="stats-grid">
            <div class="stat-card">
                <h3>Total Shows</h3>
                <div class="value">{total_shows}</div>
            </div>
            <div class="stat-card">
                <h3>Anime</h3>
                <div class="value">{anime_count}</div>
            </div>
            <div class="stat-card">
                <h3>Series</h3>
                <div class="value">{series_count}</div>
            </div>
            <div class="stat-card">
                <h3>Movies</h3>
                <div class="value">{movie_count}</div>
            </div>
            <div class="stat-card">
                <h3>Total Episodes</h3>
                <div class="value">{total_episodes}</div>
            </div>
            <div class="stat-card">
                <h3>Total Servers</h3>
                <div class="value">{total_servers}</div>
            </div>
            <div class="stat-card">
                <h3>Avg Rating</h3>
                <div class="value">{avg_rating:.1f}</div>
            </div>
            <div class="stat-card">
                <h3>Shows with Trailers</h3>
                <div class="value">{shows_with_trailers}</div>
            </div>
        </div>
        </div></body></html>
        """
        return Response(html, mimetype='text/html')
    except Exception as e:
        return f"<h2>Error loading stats</h2><p>{e}</p>", 500

@app.route('/download-db')
def download_db():
    """Download database file"""
    db_path = GLOBAL_STATE['db_path']
    if not os.path.exists(db_path):
        return "Database file not found.", 404
    
    try:
        return send_file(db_path, as_attachment=True, download_name='scraper.db')
    except Exception as e:
        return f"Error downloading database: {e}", 500

@app.route('/api/status')
def api_status():
    return jsonify(GLOBAL_STATE)

@app.route('/api/start', methods=['POST'])
def api_start():
    global SCRAPER_THREAD
    if not GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE['scraper_running'] = True
        GLOBAL_STATE['stop_scraper'] = False
        
        GLOBAL_STATE['stats'] = {
            "total_sources": 0, "total_pending": 0, "completed": 0, "failed": 0,
            "series": 0, "movies": 0, "anime": 0, "current_file": "N/A", "failed_urls": []
        }
        
        log_message("Scraper starting...", level="info")
        
        try:
            db = Database(GLOBAL_STATE['db_path'])
            all_urls_map = db.get_all_urls_from_files(GLOBAL_STATE['json_files'])
            db.get_pending_urls(all_urls_map)
        except Exception as e:
            log_message(f"Error pre-calculating totals: {e}", level="error")

        SCRAPER_THREAD = threading.Thread(target=run_scraper_task, daemon=True)
        SCRAPER_THREAD.start()
        return jsonify({"success": True, "message": "Scraper started."})
    return jsonify({"success": False, "message": "Scraper already running."})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    if GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE['stop_scraper'] = True
        log_message("Stop signal sent. Waiting for current task to finish...", level="warning")
        return jsonify({"success": True, "message": "Stop signal sent."})
    return jsonify({"success": False, "message": "Scraper not running."})

# --- Main Execution ---

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    
    for f in GLOBAL_STATE['json_files']:
        if not os.path.exists(f):
            try:
                with open(f, 'w') as new_file:
                    json.dump({"urls": []}, new_file)
            except Exception as e:
                print(f"Could not create file {f}: {e}")

    init_database(GLOBAL_STATE['db_path'])
    
    print("DASHBOARD OPEN !")
    print(f"Access at: http://127.0.0.1:8080")
    
    app.run(host='0.0.0.0', port=8080, debug=False)
