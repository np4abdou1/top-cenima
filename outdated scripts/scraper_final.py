"""
TopCinema Scraper with Flask Web Dashboard
Replaces 01_init_database.py and 02_scraper_with_db.py

This single file runs a Flask server to provide:
- A web UI to start/stop the scraper.
- Real-time stats and logging.
- A list of failed URLs.
- A database viewer.
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

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, Response, request

# --- Global State Management ---

# Suppress flask logging to keep terminal clean
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

GLOBAL_STATE: Dict[str, Any] = {
    "scraper_running": False,
    "stop_scraper": False,
    "log_message": "Scraper is idle. Press 'Start' to begin.",
    "stats": {
        "total_sources": 0,
        "total_pending": 0,
        "completed": 0,
        "failed": 0,
        "series": 0,
        "movies": 0,
        "current_file": "N/A",
        "failed_urls": []  # List of {'url': str, 'error': str}
    },
    "json_files": ["data/series_animes.json", "data/movies.json"],
    "db_path": "data/scraper.db"
}
SCRAPER_THREAD = None

# --- Database Initialization (from 01_init_database.py) ---

def init_database(db_path: str = "data/scraper.db"):
    """Create 4-table database schema with progress tracking"""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # TABLE 1: SHOWS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS shows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL UNIQUE,
        slug TEXT UNIQUE,
        type TEXT NOT NULL CHECK(type IN ('movie', 'series')),
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

    # TABLE 2: SEASONS
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

    # TABLE 3: EPISODES
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

    # TABLE 4: SERVERS
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

    # PROGRESS TABLE
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

# --- Database Class (from 02_scraper_with_db.py) ---

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_connection(self):
        """Get a thread-safe connection."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    
    def slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug"""
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
            
            # Force type to 'movie' or 'series' as 'anime' is not in schema
            show_type = show_data.get("type", "series")
            if show_type == "anime":
                show_type = "series"

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
            # Create a dummy season and episode for movies
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
        """Loads all URLs from all specified JSON files."""
        all_urls_map = {}
        total_count = 0
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                file_urls = data if isinstance(data, list) else data.get("urls", [])
                if file_path.endswith("series_animes.json"):
                    # Handle the specific format from user prompt
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
        """Filters the URL map to only include pending URLs."""
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
        """Initialize progress tracking for URLs"""
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

# --- Scraper Logic (from 02_scraper_with_db.py) ---

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
VERIFY_SSL = False # As per original script logic

# Setup persistent session
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
    """Update global state log"""
    timestamp = time.strftime("%H:%M:%S")
    level_map = {"info": "INFO", "success": "SUCCESS", "warning": "WARN", "error": "ERROR"}
    GLOBAL_STATE['log_message'] = f"[{timestamp}] [{level_map.get(level, 'INFO')}] {msg}"
    
    # Also print to console for debugging if needed, but user wanted clean terminal
    # print(GLOBAL_STATE['log_message']) 

def fetch_html(url: str) -> Optional[BeautifulSoup]:
    """Fetch and parse HTML"""
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
    """Extract number from Arabic or English text"""
    if not text: return None
    m = REGEX_PATTERNS['number'].search(text)
    if m: return int(m.group(1))
    lower = text.replace("ي", "ى").replace("أ", "ا").replace("إ", "ا").strip()
    for word, num in ARABIC_ORDINALS.items():
        if word in lower: return num
    return None

def clean_title(title: str) -> str:
    """Remove prefixes and suffixes from titles"""
    if not title: return title
    cleaned = REGEX_PATTERNS['title_clean_prefix'].sub('', title)
    prev = ""
    while prev != cleaned:
        prev = cleaned
        cleaned = REGEX_PATTERNS['title_clean_suffix'].sub(' ', cleaned)
    cleaned = ' '.join(cleaned.split()).strip(' -–—|:،؛')
    return cleaned

def get_trailer_embed_url(page_url: str, form_url: str) -> Optional[str]:
    """Fetch trailer"""
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
            return iframe["src"].strip()
        return None
    except Exception as e:
        log_message(f"Trailer fetch error: {str(e)[:50]}", level="warning")
        return None

def get_episode_servers(episode_id: str, referer: Optional[str] = None, total_servers: int = 10) -> List[Dict]:
    """Fetch streaming servers for an episode"""
    if GLOBAL_STATE['stop_scraper']: return []
    servers: List[Dict] = []
    base = "https://web7.topcinema.cam"
    if referer:
        try:
            p = urlparse(referer)
            base = f"{p.scheme}://{p.netloc}"
        except Exception:
            pass # Use default base
    
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
    """Extract episode ID from watch page HTML"""
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
    """Scrape all episodes from a season page"""
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
    """Shared function for extracting media details"""
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
    """Scrape series"""
    if GLOBAL_STATE['stop_scraper']: return None
    soup = fetch_html(url)
    if not soup: return None
    
    details = extract_media_details(soup)
    seasons: List[Dict] = []
    season_urls: Dict[int, str] = {}
    seen_urls = set()
    
    # Prioritize 'Small--Box Season'
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

    # Fallback to other links
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

    # If still no seasons, assume current page is season 1
    if not seasons:
        season_urls[1] = url
        seasons.append({"season_number": 1, "poster": details["poster"], "episodes": []})

    for season in seasons:
        if GLOBAL_STATE['stop_scraper']: break
        s_num = season["season_number"]
        if s_num in season_urls:
            season["episodes"] = scrape_season_episodes(season_urls[s_num])

    trailer_url = get_trailer_embed_url(url, url)

    return {
        "title": details["title"], "type": "series", "imdb_rating": details["imdb_rating"],
        "poster": details["poster"], "synopsis": details["synopsis"], "metadata": details["metadata"],
        "trailer": trailer_url, "source_url": url, "seasons": seasons
    }

def scrape_movie(url: str) -> Optional[Dict]:
    """Scrape movie"""
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
    """Main scraping function, modified to force type"""
    if GLOBAL_STATE['stop_scraper']: return None
    url = url_input.strip()
    
    result: Optional[Dict] = None
    
    # Apply force_type logic
    if force_type == 'series':
        result = scrape_series(url)
    elif REGEX_PATTERNS['movie'].search(url):
        result = scrape_movie(url)
    else:
        result = scrape_series(url)
    
    # Ensure the forced type is set in the final result
    if result and force_type:
        result['type'] = force_type
    
    return result

# --- Scraper Control Thread ---

def run_scraper_task():
    """The main scraper task that runs in a thread."""
    try:
        db = Database(GLOBAL_STATE['db_path'])
        
        # 1. Load all URLs from all files
        all_urls_map = db.get_all_urls_from_files(GLOBAL_STATE['json_files'])
        all_urls_list = [url for urls in all_urls_map.values() for url in urls]
        
        # 2. Get *only* pending URLs
        pending_urls_map = db.get_pending_urls(all_urls_map)
        
        # 3. Initialize progress for any *new* URLs
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
            
            # Determine if we need to force the type
            current_force_type = "series" if "series_animes.json" in json_file else None
            
            for idx, url in enumerate(urls, 1):
                if GLOBAL_STATE['stop_scraper']:
                    log_message("Scraper stop signal received.", level="warning")
                    break
                
                log_message(f"[{idx}/{len(urls)}] Scraping: {url}", level="info")
                
                try:
                    result = run_single(url, force_type=current_force_type)
                    if result:
                        show_id = db.insert_show(result)
                        if show_id:
                            if result.get("type") == "series":
                                db.insert_seasons_and_episodes(show_id, result.get("seasons", []))
                                GLOBAL_STATE['stats']['series'] += 1
                            else: # movie
                                db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                                GLOBAL_STATE['stats']['movies'] += 1
                            
                            db.mark_progress(url, "completed", show_id)
                            GLOBAL_STATE['stats']['completed'] += 1
                        else:
                            # Failed to insert (likely duplicate)
                            db.mark_progress(url, "failed", error="Duplicate or DB error")
                            GLOBAL_STATE['stats']['failed'] += 1
                            GLOBAL_STATE['stats']['failed_urls'].append({"url": url, "error": "Duplicate or DB Error"})
                    else:
                        # Scraping returned no data
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

# --- Flask Web Server ---

app = Flask(__name__)

@app.route('/')
def index():
    """Serves the main dashboard HTML."""
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
            .container { max-width: 900px; margin: 0 auto; background-color: #1E1E1E; border: 1px solid #333;
                         border-radius: 8px; padding: 25px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
            h1 { color: #FFFFFF; border-bottom: 2px solid #444; padding-bottom: 10px; margin-top: 0; }
            h2 { color: #E0E0E0; border-bottom: 1px solid #333; padding-bottom: 8px; margin-top: 30px; }
            .controls { margin-bottom: 20px; display: flex; gap: 15px; }
            button { background-color: #333; color: #E0E0E0; border: 1px solid #555; padding: 12px 20px;
                     border-radius: 5px; cursor: pointer; font-size: 16px; transition: all 0.2s ease; }
            button:hover { background-color: #444; border-color: #777; }
            button:disabled { background-color: #2a2a2a; color: #555; border-color: #444; cursor: not-allowed; }
            #start-btn { background-color: #28a745; border-color: #28a745; color: #FFFFFF; }
            #start-btn:hover { background-color: #218838; }
            #stop-btn { background-color: #dc3545; border-color: #dc3545; color: #FFFFFF; }
            #stop-btn:hover { background-color: #c82333; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; }
            .stat-box { background-color: #2a2a2a; border: 1px solid #333; border-radius: 5px; padding: 15px; }
            .stat-box strong { display: block; font-size: 24px; color: #FFFFFF; }
            .stat-box span { font-size: 14px; color: #AAA; }
            pre#log-message { background-color: #2a2a2a; border: 1px solid #333; padding: 15px; border-radius: 5px;
                             white-space: pre-wrap; word-wrap: break-word; color: #E0E0E0; font-family: "Courier New", Courier, monospace;
                             min-height: 2.5em; line-height: 1.6; }
            #failed-urls { max-height: 300px; overflow-y: auto; background: #2a2a2a; border: 1px solid #333;
                           border-radius: 5px; padding: 0 15px; }
            #failed-urls div { border-bottom: 1px solid #333; padding: 10px 0; }
            #failed-urls div:last-child { border-bottom: none; }
            #failed-urls code { color: #dc3545; }
            #failed-urls span { color: #AAA; display: block; font-size: 0.9em; }
            nav { margin-bottom: 20px; }
            nav a { color: #3498db; text-decoration: none; padding: 5px 10px; border-radius: 4px; }
            nav a:hover { background-color: #333; }
            nav a.active { font-weight: bold; background-color: #2a2a2a; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>TopCinema Scraper Dashboard</h1>
            <nav>
                <a href="/" class="active">Dashboard</a> | <a href="/db-view">DB Viewer</a>
            </nav>
            <div class="controls">
                <button id="start-btn">Start Scraper</button>
                <button id="stop-btn" disabled>Stop Scraper</button>
            </div>
            
            <h2>Live Stats</h2>
            <p>Current File: <strong id="current-file">N/A</strong></p>
            <div class="stats-grid">
                <div class="stat-box"><strong id="pending">0</strong><span>Pending</span></div>
                <div class="stat-box"><strong id="completed">0</strong><span>Completed</span></div>
                <div class="stat-box"><strong id="failed">0</strong><span>Failed</span></div>
                <div class="stat-box"><strong id="series">0</strong><span>Series</span></div>
                <div class="stat-box"><strong id="movies">0</strong><span>Movies</span></div>
                <div class="stat-box"><strong id="total">0</strong><span>Total Sources</span></div>
            </div>

            <h2>Live Log</h2>
            <pre id="log-message">Waiting for status...</pre>

            <h2>Failed URLs</h2>
            <div id="failed-urls"><p>No failed URLs yet.</p></div>
        </div>

        <script>
            const startBtn = document.getElementById('start-btn');
            const stopBtn = document.getElementById('stop-btn');
            const logMsg = document.getElementById('log-message');
            const pending = document.getElementById('pending');
            const completed = document.getElementById('completed');
            const failed = document.getElementById('failed');
            const series = document.getElementById('series');
            const movies = document.getElementById('movies');
            const total = document.getElementById('total');
            const currentFile = document.getElementById('current-file');
            const failedUrlsDiv = document.getElementById('failed-urls');

            async function fetchStatus() {
                try {
                    const response = await fetch('/api/status');
                    const data = await response.json();
                    
                    // Update buttons
                    startBtn.disabled = data.scraper_running;
                    stopBtn.disabled = !data.scraper_running;

                    // Update log
                    logMsg.textContent = data.log_message;

                    // Update stats
                    const stats = data.stats;
                    pending.textContent = stats.total_pending - stats.completed - stats.failed;
                    completed.textContent = stats.completed;
                    failed.textContent = stats.failed;
                    series.textContent = stats.series;
                    movies.textContent = stats.movies;
                    total.textContent = stats.total_sources;
                    currentFile.textContent = stats.current_file;

                    // Update failed URLs
                    if (stats.failed_urls.length > 0) {
                        failedUrlsDiv.innerHTML = stats.failed_urls.map(item => 
                            `<div>
                                <code>${item.url}</code>
                                <span>Error: ${item.error}</span>
                            </div>`
                        ).join('');
                    } else {
                        failedUrlsDiv.innerHTML = '<p>No failed URLs yet.</p>';
                    }
                } catch (e) {
                    logMsg.textContent = 'Error fetching status. Server might be down.';
                }
            }

            startBtn.addEventListener('click', async () => {
                try {
                    await fetch('/api/start', { method: 'POST' });
                    fetchStatus(); // Immediately update
                } catch (e) {
                    logMsg.textContent = 'Error starting scraper.';
                }
            });

            stopBtn.addEventListener('click', async () => {
                try {
                    await fetch('/api/stop', { method: 'POST' });
                    fetchStatus(); // Immediately update
                } catch (e) {
                    logMsg.textContent = 'Error stopping scraper.';
                }
            });

            setInterval(fetchStatus, 2000); // Poll every 2 seconds
            fetchStatus(); // Initial fetch
        </script>
    </body>
    </html>
    """
    return Response(html, mimetype='text/html')

@app.route('/db-view')
def db_view():
    """Serves the database viewer HTML."""
    db_path = GLOBAL_STATE['db_path']
    if not os.path.exists(db_path):
        return "Database file not found. Run the scraper to create it.", 404

    html = """
    <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>DB Viewer</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
               background-color: #121212; color: #E0E0E0; margin: 0; padding: 20px; }
        .container { max-width: 900px; margin: 0 auto; background-color: #1E1E1E; border: 1px solid #333;
                     border-radius: 8px; padding: 25px; }
        h1 { color: #FFFFFF; border-bottom: 2px solid #444; padding-bottom: 10px; margin-top: 0; }
        h2 { color: #E0E0E0; border-bottom: 1px solid #333; padding-bottom: 8px; margin-top: 30px; }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { border: 1px solid #333; padding: 10px; text-align: left; }
        th { background-color: #2a2a2a; color: #FFFFFF; }
        tr:nth-child(even) { background-color: #2a2a2a; }
        nav { margin-bottom: 20px; }
        nav a { color: #3498db; text-decoration: none; padding: 5px 10px; border-radius: 4px; }
        nav a:hover { background-color: #333; }
        nav a.active { font-weight: bold; background-color: #2a2a2a; }
    </style>
    </head><body><div class="container">
    <h1>Database Viewer</h1>
    <nav><a href="/">Dashboard</a> | <a href="/db-view" class="active">DB Viewer</a></nav>
    """

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row['name'] for row in cursor.fetchall()]
        
        table_counts = {}
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                table_counts[table] = cursor.fetchone()['count']
            except sqlite3.Error:
                table_counts[table] = "N/A"

        html += "<h2>Table Counts</h2><table><tr><th>Table Name</th><th>Row Count</th></tr>"
        for table, count in table_counts.items():
            html += f"<tr><td>{table}</td><td>{count}</td></tr>"
        html += "</table>"

        html += "<h2>Table Schemas</h2>"
        for table in tables:
            html += f"<h3>Schema: <code>{table}</code></h3>"
            cursor.execute(f"PRAGMA table_info({table});")
            schema = cursor.fetchall()
            html += "<table><tr><th>Column</th><th>Type</th><th>Not Null</th><th>Default</th><th>PK</th></tr>"
            for col in schema:
                html += f"<tr><td>{col['name']}</td><td>{col['type']}</td><td>{col['notnull']}</td><td>{col['dflt_value']}</td><td>{col['pk']}</td></tr>"
            html += "</table>"

        conn.close()
    except Exception as e:
        html += f"<h2>Error loading database</h2><p>{e}</p>"

    html += "</div></body></html>"
    return Response(html, mimetype='text/html')

@app.route('/api/status')
def api_status():
    """Returns the current global state as JSON."""
    return jsonify(GLOBAL_STATE)

@app.route('/api/start', methods=['POST'])
def api_start():
    """Starts the scraper thread."""
    global SCRAPER_THREAD
    if not GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE['scraper_running'] = True
        GLOBAL_STATE['stop_scraper'] = False
        
        # Reset stats
        GLOBAL_STATE['stats'] = {
            "total_sources": 0, "total_pending": 0, "completed": 0, "failed": 0,
            "series": 0, "movies": 0, "current_file": "N/A", "failed_urls": []
        }
        
        log_message("Scraper starting...", level="info")
        
        # Pre-calculate totals
        try:
            db = Database(GLOBAL_STATE['db_path'])
            all_urls_map = db.get_all_urls_from_files(GLOBAL_STATE['json_files'])
            db.get_pending_urls(all_urls_map) # This updates GLOBAL_STATE['stats']['total_pending']
        except Exception as e:
            log_message(f"Error pre-calculating totals: {e}", level="error")

        SCRAPER_THREAD = threading.Thread(target=run_scraper_task, daemon=True)
        SCRAPER_THREAD.start()
        return jsonify({"success": True, "message": "Scraper started."})
    return jsonify({"success": False, "message": "Scraper already running."})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    """Signals the scraper thread to stop."""
    if GLOBAL_STATE['scraper_running']:
        GLOBAL_STATE['stop_scraper'] = True
        log_message("Stop signal sent. Waiting for current task to finish...", level="warning")
        return jsonify({"success": True, "message": "Stop signal sent."})
    return jsonify({"success": False, "message": "Scraper not running."})

# --- Main Execution ---

if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Check for source JSON files
    for f in GLOBAL_STATE['json_files']:
        if not os.path.exists(f):
            print(f"WARNING: Source file not found: {f}. Scraper may not find items.")
            # Create empty files if they don't exist, as per user's repo structure
            if not os.path.exists(f):
                try:
                    with open(f, 'w') as new_file:
                        json.dump({"urls": []}, new_file)
                    print(f"Created empty file: {f}")
                except Exception as e:
                    print(f"Could not create file {f}: {e}")

    # Initialize the database schema
    init_database(GLOBAL_STATE['db_path'])
    
    # Print the requested message
    print("DASHBOARD OPEN !")
    print(f"Access at: http://127.0.0.1:8080")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=8080, debug=False)
