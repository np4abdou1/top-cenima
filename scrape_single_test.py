import json
import os
import re
import time
from typing import List, Dict, Optional, Tuple
from urllib.parse import unquote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

console = Console()

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# ANSI color codes for modern terminal logging
class Colors:
    RESET = '\033[0m'
    
    # Foreground colors
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    CYAN = '\033[36m'

# Compile regex patterns once for performance
REGEX_PATTERNS = {
    'number': re.compile(r'(\d+)'),
    'movie': re.compile(r'(\/فيلم-|\/film-|\/movie-|%d9%81%d9%8a%d9%84%d9%85)', re.IGNORECASE),
    'episode': re.compile(r'(?:الحلقة|Episode)\s*(\d+)'),
    'watch_suffix': re.compile(r'/watch/?$'),
    'title_prefix': re.compile(r'^(انمي|مسلسل)\s+'),
    'episode_id': re.compile(r'"id"\s*:\s*"(\d+)"'),
    # Enhanced title cleaning - removes all common Arabic prefixes/suffixes
    'title_clean_prefix': re.compile(
        r'^\s*(فيلم|انمي|مسلسل|anime|film|movie|series)\s+',
        re.IGNORECASE | re.UNICODE
    ),
    'title_clean_suffix': re.compile(
        r'\s+(مترجم|اون\s*لاين|اونلاين|online|مترجمة|مدبلج|مدبلجة)(\s+|$)',
        re.IGNORECASE | re.UNICODE
    )
}

def print_banner():
    """Print styled banner with Rich"""
    banner_text = Text("🔥 TopCinema Scraper v2.0 🔥", style="bold blue")
    subtitle = Text("By Abdelrahman", style="dim")
    
    banner = Panel.fit(
        banner_text + "\n" + subtitle,
        border_style="blue",
        padding=(1, 4)
    )
    console.print(banner)

def log(msg: str, level: str = "info") -> None:
    """Enhanced logging with Rich"""
    timestamp = time.strftime("%H:%M:%S")
    
    if level == "info":
        console.print(f"[{timestamp}] {msg}", style="dim")
    elif level == "success":
        console.print(f"[{timestamp}] {msg}", style="bold green")
    elif level == "warning":
        console.print(f"[{timestamp}] {msg}", style="bold yellow")
    elif level == "error":
        console.print(f"[{timestamp}] {msg}", style="bold red")
    elif level == "trailer":
        console.print(f"[{timestamp}] 🎬 {msg}", style="cyan")
    elif level == "season":
        console.print(f"[{timestamp}] 📺 {msg}", style="cyan")
    elif level == "debug":
        pass  # Hide debug logs
    else:
        console.print(f"[{timestamp}] {msg}")

class EpisodeLogger:
    """Minimal episode logging"""
    def __init__(self):
        self.episodes_found = {}
        self.episodes_logged = set()
        self.next_expected = 1
    
    def add_episode(self, ep_num: int, server_count: int):
        """Add an episode to the logging queue"""
        self.episodes_found[ep_num] = server_count
        self._try_log_episodes()
    
    def _try_log_episodes(self):
        """Log episodes in order"""
        while self.next_expected in self.episodes_found and self.next_expected not in self.episodes_logged:
            ep_num = self.next_expected
            server_count = self.episodes_found[ep_num]
            console.print(f"Episode {ep_num:2d} - {server_count:2d} servers", style="cyan")
            self.episodes_logged.add(ep_num)
            self.next_expected += 1
    
    def finalize(self):
        """Log any remaining episodes"""
        remaining = set(self.episodes_found.keys()) - self.episodes_logged
        if remaining:
            console.print("Additional episodes:", style="cyan")
            for ep_num in sorted(remaining):
                server_count = self.episodes_found[ep_num]
                console.print(f"Episode {ep_num:2d} - {server_count:2d} servers", style="cyan")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://web7.topcinema.cam/",
    "Upgrade-Insecure-Requests": "1",
}

# Generic headers without cookies (more reliable)
TRAILER_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ar;q=0.8",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-requested-with": "XMLHttpRequest"
}

REQUEST_TIMEOUT = 15  # Reduced from 30 for faster failures
REQUEST_DELAY = 0.3  # Reduced from 1 for faster scraping

# Configure session with aggressive connection pooling and fast retry
SESSION = requests.Session()
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    
    # Aggressive retry strategy - fail fast
    retry_strategy = Retry(
        total=2,  # Reduced from 3
        backoff_factor=0.5,  # Reduced from 1
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST"],
        raise_on_status=False
    )
    
    # Increased pool size for better concurrency
    adapter = HTTPAdapter(
        pool_connections=50,  # Increased from 32
        pool_maxsize=100,  # Increased from 64
        max_retries=retry_strategy,
        pool_block=False
    )
    SESSION.mount('https://', adapter)
    SESSION.mount('http://', adapter)
    
    # Disable SSL warnings for speed
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception as e:
    logger.warning(f"Failed to configure session adapter: {e}")

# SSL verification - configurable via environment variable
VERIFY_SSL = os.environ.get('VERIFY_SSL', 'true').lower() == 'true'

ARABIC_ORDINALS = {
    "الاول": 1, "الأول": 1, "الثاني": 2, "ثاني": 2,
    "الثالث": 3, "ثالث": 3, "الرابع": 4, "رابع": 4,
    "الخامس": 5, "خامس": 5, "السادس": 6, "سادس": 6,
    "السابع": 7, "سابع": 7, "الثامن": 8, "ثامن": 8,
    "التاسع": 9, "تاسع": 9, "العاشر": 10, "عاشر": 10,
}

def fetch_html(url: str) -> Optional[BeautifulSoup]:
    """Fetch and parse HTML with improved error handling"""
    if not url.startswith(('http://', 'https://')):
        logger.error(f"Invalid URL scheme: {url}")
        return None
        
    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.exceptions.Timeout:
        log(f"Timeout fetching {url}", level="error")
    except requests.exceptions.HTTPError as e:
        log(f"HTTP error {e.response.status_code} for {url}", level="error")
    except requests.exceptions.RequestException as e:
        log(f"Request failed for {url}: {str(e)[:50]}", level="error")
    except Exception as e:
        log(f"Unexpected error fetching {url}: {str(e)[:50]}", level="error")
    return None

def extract_number_from_text(text: str) -> Optional[int]:
    """Extract number from Arabic or English text"""
    if not text:
        return None
    m = REGEX_PATTERNS['number'].search(text)
    if m:
        return int(m.group(1))
    lower = text.replace("ي", "ى").replace("أ", "ا").replace("إ", "ا").strip()
    for word, num in ARABIC_ORDINALS.items():
        if word in lower:
            return num
    return None

def clean_title(title: str) -> str:
    """Remove prefixes and suffixes from titles - Enhanced version"""
    if not title:
        return title
    
    # Remove prefix (فيلم, انمي, مسلسل, etc.)
    cleaned = REGEX_PATTERNS['title_clean_prefix'].sub('', title)
    
    # Remove suffix (مترجم, اون لاين, etc.) - apply multiple times to catch all
    prev = ""
    while prev != cleaned:
        prev = cleaned
        cleaned = REGEX_PATTERNS['title_clean_suffix'].sub(' ', cleaned)
    
    # Additional cleanup - remove multiple spaces and trim
    cleaned = ' '.join(cleaned.split())
    
    # Remove trailing/leading special chars
    cleaned = cleaned.strip(' -–—|:،؛')
    
    return cleaned

def get_trailer_embed_url(page_url: str, form_url: str) -> Optional[str]:
    """Fetch trailer with separate URLs for page and form data"""
    try:
        p = urlparse(page_url)
        base = f"{p.scheme}://{p.netloc}"
        trailer_endpoint = base + "/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
        
        # Use form_url exactly as provided (no modification)
        # Use safe=':/' to preserve URL structure and encoding='utf-8' for Arabic
        data = f"href={requests.utils.quote(form_url, safe=':/', encoding='utf-8')}"
        log(f"Using form data URL: {form_url}", level="debug")
        
        # Exact headers that worked in curl
        trailer_headers = {
            "accept": "*/*",
            "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "x-requested-with": "XMLHttpRequest",
            "referer": page_url
        }
        
        resp = SESSION.post(trailer_endpoint, 
                          headers=trailer_headers, 
                          data=data,
                          timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        
        log(f"Trailer endpoint response: {resp.text[:200]}...", level="debug")
        
        soup = BeautifulSoup(resp.text, "html.parser")
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            trailer_src = iframe["src"].strip()
            log(f"Found trailer iframe src: {trailer_src}", level="debug")
            return trailer_src
        
        log("No valid iframe src found in response", level="debug")
        return None
    except UnicodeEncodeError as e:
        log(f"Trailer encoding error (Arabic chars): {str(e)[:60]}", level="debug")
        return None
    except Exception as e:
        log(f"Trailer fetch error: {str(e)[:60]}", level="debug")
        return None

def get_episode_servers(episode_id: str, referer: Optional[str] = None, total_servers: int = 10) -> List[Dict]:
    """Fetch streaming servers for an episode via PHP endpoint"""
    servers: List[Dict] = []
    if referer:
        p = urlparse(referer)
        base = f"{p.scheme}://{p.netloc}"
    else:
        base = "https://web7.topcinema.cam"
    
    server_url = base + "/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    server_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
    }
    if referer:
        server_headers["Referer"] = referer

    def fetch_one(i: int):
        try:
            data = {"id": str(episode_id), "i": str(i)}
            resp = SESSION.post(server_url, headers=server_headers, data=data, timeout=5)  # Reduced from 8
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            iframe = soup.find("iframe")
            if iframe and iframe.get("src") and iframe.get("src").strip():
                return {"server_number": i, "embed_url": iframe.get("src").strip()}
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=min(12, total_servers)) as ex:  # Increased from 8
        futures = {ex.submit(fetch_one, i): i for i in range(total_servers)}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                servers.append(res)

    servers.sort(key=lambda x: x.get("server_number", 0))
    return servers

def extract_episode_id_from_watch_page(soup: BeautifulSoup) -> Optional[str]:
    """Extract episode ID from watch page HTML"""
    if not soup:
        return None
    
    li = soup.select_one(".watch--servers--list li.server--item[data-id]")
    if li and li.has_attr("data-id"):
        return li["data-id"].strip()
    
    for script in soup.find_all("script"):
        if script.string:
            m = REGEX_PATTERNS['episode_id'].search(script.string)
            if m:
                return m.group(1)
    return None

def scrape_season_episodes(season_url: str) -> List[Dict]:
    """Scrape all episodes from a season page"""
    list_url = season_url.rstrip('/') + '/list/' if not season_url.endswith('/list/') else season_url
    
    soup = fetch_html(list_url)
    if not soup:
        return []

    episodes: List[Dict] = []
    seen = set()
    episode_logger = EpisodeLogger()

    # Find episode links
    anchors = soup.select('.allepcont .row > a')
    if not anchors:
        anchors = [x for x in soup.find_all('a') if (x.find(class_='epnum') or (x.get('title') and 'الحلقة' in x.get('title')))]

    log(f"Found {len(anchors)} episode links to process", level="info")

    def process_episode(a):
        try:
            raw_href = a.get('href')
            ep_title = a.get('title', '')
            
            # Extract episode number
            ep_num = None
            em = a.find('em')
            if em:
                m = REGEX_PATTERNS['number'].search(em.get_text())
                if m:
                    ep_num = int(m.group(1))
            
            if not ep_num:
                epdiv = a.find(class_='epnum')
                if epdiv:
                    m = REGEX_PATTERNS['number'].search(epdiv.get_text())
                    if m:
                        ep_num = int(m.group(1))
            
            if not ep_num:
                src_text = ep_title or ''
                h2 = a.find('h2')
                if h2 and not src_text:
                    src_text = h2.get_text(strip=True)
                m = REGEX_PATTERNS['episode'].search(src_text)
                if m:
                    ep_num = int(m.group(1))
            
            if not ep_num or ep_num == 0:
                ep_num = 999
            
            # Get poster
            ep_poster = None
            img = a.find('img')
            if img:
                ep_poster = img.get('src') or img.get('data-src')
            
            key = (str(ep_num) or ep_title or raw_href or '').strip()
            if key in seen:
                return None
            seen.add(key)
            
            # Fetch watch page
            watch_url = raw_href
            if not REGEX_PATTERNS['watch_suffix'].search(raw_href):
                watch_url = raw_href.rstrip('/') + '/watch/'
            
            ep_watch_soup = fetch_html(watch_url)
            episode_id = None
            
            if ep_watch_soup:
                episode_id = extract_episode_id_from_watch_page(ep_watch_soup)
            
            server_list: List[Dict] = []
            if episode_id:
                server_list = get_episode_servers(episode_id, referer=watch_url, total_servers=10)
            
            # Log episode using the episode logger
            if ep_num != 999:
                episode_logger.add_episode(ep_num, len(server_list))
            
            return {
                "episode_number": ep_num,
                "servers": server_list
            }
        except Exception as e:
            log(f"Error processing episode: {str(e)[:50]}", level="error")
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:  # Increased from 6
        try:
            for res in ex.map(process_episode, anchors):
                if res:
                    episodes.append(res)
        except Exception as e:
            log(f"Error in thread pool: {str(e)}", level="error")
            raise

    # Finalize episode logging
    episode_logger.finalize()

    episodes.sort(key=lambda e: e.get("episode_number", 999))
    
    episodes = [e for e in episodes if e.get("episode_number", 999) != 999]
    
    return episodes

def scrape_series(url: str) -> Optional[Dict]:
    """Scrape series with correct episode URL format"""
    soup = fetch_html(url)
    if not soup:
        return None
    
    details = extract_media_details(soup, "series")

    # Scrape episodes for all seasons first
    seasons: List[Dict] = []
    season_urls: Dict[int, str] = {}  # Track URLs temporarily
    seen_urls = set()
    
    for s_el in soup.select('div.Small--Box.Season'):
        a_el = s_el.find('a')
        if not a_el or not a_el.get('href'):
            continue
        s_url = a_el.get('href')
        if s_url in seen_urls:
            continue
        seen_urls.add(s_url)
        s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
        s_num = None
        en = s_el.find('div', class_='epnum')
        if en:
            s_num = extract_number_from_text(en.get_text(" ", strip=True))
        if not s_num:
            s_num = extract_number_from_text(s_title)
        s_num = s_num or 1
        
        # Fetch season poster from season page itself
        s_poster = None
        season_soup = fetch_html(s_url)
        if season_soup:
            poster_img = season_soup.select_one('.MainSingle .left .image img')
            if poster_img:
                s_poster = poster_img.get('src') or poster_img.get('data-src')
        
        season_urls[s_num] = s_url  # Store URL temporarily
        seasons.append({
            "season_number": s_num,
            "poster": s_poster,
            "episodes": []
        })

    # Fallback: scan for season URLs
    if not seasons:
        for a_el in soup.find_all('a', href=True):
            href = a_el['href']
            if '/series/' in href and 'الموسم' in href:
                if href in seen_urls:
                    continue
                seen_urls.add(href)
                s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
                s_num = extract_number_from_text(s_title) or extract_number_from_text(href) or 1
                
                # Fetch season poster from season page
                s_poster = None
                season_soup = fetch_html(href)
                if season_soup:
                    poster_img = season_soup.select_one('.MainSingle .left .image img')
                    if poster_img:
                        s_poster = poster_img.get('src') or poster_img.get('data-src')
                
                season_urls[s_num] = href  # Store URL temporarily
                seasons.append({
                    "season_number": s_num,
                    "poster": s_poster,
                    "episodes": []
                })

    # Sort seasons
    seasons.sort(key=lambda s: s.get('season_number', 0))

    if not seasons:
        season_urls[1] = url
        seasons.append({
            "season_number": 1,
            "poster": details["poster"],
            "episodes": []
        })

    # Scrape episodes for all seasons
    for season in seasons:
        s_num = season["season_number"]
        if s_num in season_urls:
            season["episodes"] = scrape_season_episodes(season_urls[s_num])

    # Get first episode URL for trailer (temporary, not stored)
    episode_page_url = None
    if seasons:
        first_season_url = list(seen_urls)[0] if seen_urls else None
        if first_season_url:
            temp_soup = fetch_html(first_season_url)
            if temp_soup:
                first_ep_link = temp_soup.select_one(".allepcont .row > a")
                if first_ep_link:
                    episode_page_url = first_ep_link.get("href")
            
    # Fetch trailer using exact episode page URL
    trailer_url = None
    if episode_page_url:
        log(f"Using exact episode URL for trailer: {episode_page_url}", level="debug")
        trailer_url = get_trailer_embed_url(url, episode_page_url)
    
    # Now try trailer fetch using first episode's WATCH page URL
    if trailer_url:
        log("Trailer found", level="success")
    else:
        log("No trailer available", level="info")

    return {
        "title": details["title"],
        "type": "series",
        "imdb_rating": details["imdb_rating"],
        "poster": details["poster"],
        "synopsis": details["synopsis"],
        "metadata": details["metadata"],
        "trailer": trailer_url,
        "seasons": seasons
    }

def scrape_movie(url: str) -> Optional[Dict]:
    """Scrape movie with proper trailer URL handling"""
    if not REGEX_PATTERNS['movie'].search(url):
        log(f"URL {url} doesn't appear to be a movie", level="error")
        return None
    
    log(f"Fetching movie: {url}")
    details_soup = fetch_html(url)
    if not details_soup:
        return None
    
    details = extract_media_details(details_soup, "movie")
    
    # Get servers using get_episode_servers (same functionality)
    watch_url = url.rstrip('/') + '/watch/'
    watch_soup = fetch_html(watch_url)
    if not watch_soup:
        return None
        
    episode_id = extract_episode_id_from_watch_page(watch_soup)
    servers = get_episode_servers(episode_id, referer=watch_url) if episode_id else []
    
    # For movies, use original URL without /watch/
    trailer_url = get_trailer_embed_url(url, url)  # Pass same URL for page and form data
    
    if trailer_url:
        log("Trailer found", level="success")
    else:
        log("No trailer available", level="info")

    title = details["title"]
    imdb_rating = details["imdb_rating"]
    poster_url = details["poster"]
    story_txt = details["synopsis"]

    return {
        "title": title,
        "type": "movie",
        "year": extract_number_from_text(title),
        "imdb_rating": imdb_rating,
        "poster": poster_url,
        "synopsis": story_txt,
        "metadata": details["metadata"],
        "trailer": trailer_url,
        "streaming_servers": servers,
        "scraped_at": datetime.now().isoformat()
    }

def extract_media_details(soup: BeautifulSoup, media_type: str) -> Dict:
    """Shared function for extracting media details"""
    details = {
        "title": "Unknown",
        "poster": None,
        "synopsis": "",
        "imdb_rating": None,
        "metadata": {}
    }
    
    try:
        # Title
        title_el = soup.find("h1", class_="post-title")
        if title_el:
            title = title_el.get_text(strip=True)
            details["title"] = clean_title(title)
        
        # Poster
        poster_wrap = soup.find('div', class_='image')
        if poster_wrap:
            img_tag = poster_wrap.find('img')
            if img_tag:
                details["poster"] = img_tag.get('src') or img_tag.get('data-src')
        
        # Story
        story = soup.find('div', class_='story')
        if story:
            p = story.find('p')
            if p:
                details["synopsis"] = p.get_text(strip=True)
        
        # IMDb rating
        imdb_box = soup.select_one(".UnderPoster .imdbR")
        if imdb_box:
            sp = imdb_box.find("span")
            if sp:
                try:
                    details["imdb_rating"] = float(sp.get_text(strip=True))
                except ValueError:
                    pass
        
        # Other details
        tax = soup.find('ul', class_='RightTaxContent')
        if tax:
            for li in tax.find_all('li'):
                key_el = li.find('span')
                if key_el:
                    key = key_el.get_text(strip=True).replace(':', '')
                    links = [a.get_text(strip=True) for a in li.find_all('a') if a.get_text(strip=True)]
                    details["metadata"][key] = links if links else li.find('strong').get_text(strip=True) if li.find('strong') else ""
    except Exception as e:
        log(f"Error extracting details: {str(e)}", level="error")
    
    # Map Arabic keys to English in metadata (fix mutation bug)
    key_mapping = {
        "قسم المسلسل": "category",
        "قسم الفيلم": "category",
        "نوع المسلسل": "genres",
        "نوع الفيلم": "genres",
        "النوع": "genres",
        "جودة المسلسل": "quality",
        "جودة الفيلم": "quality",
        "عدد الحلقات": "episode_count",
        "توقيت المسلسل": "duration",
        "توقيت الفيلم": "duration",
        "مدة الفيلم": "duration",
        "موعد الصدور": "release_year",
        "سنة الانتاج": "release_year",
        "لغة المسلسل": "language",
        "لغة الفيلم": "language",
        "دولة المسلسل": "country",
        "دولة الفيلم": "country",
        "المخرجين": "directors",
        "المخرج": "directors",
        "بطولة": "cast"
    }
    
    # Create new dict to avoid mutation during iteration
    mapped_metadata = {}
    for k, v in details["metadata"].items():
        # Clean the key (remove extra spaces and colons)
        clean_key = k.strip().rstrip(':')
        new_key = key_mapping.get(clean_key, clean_key)
        # Only add if it's a mapped key (English)
        if new_key in key_mapping.values():
            mapped_metadata[new_key] = v
    details["metadata"] = mapped_metadata
    
    return details

def run_single(url_input: str) -> Optional[Dict]:
    """Main scraping function with comprehensive movie detection"""
    url = url_input.strip()
    
    # Comprehensive movie URL pattern matching:
    # 1. Arabic movie pattern (فيلم)
    # 2. English movie pattern (film/movie)
    # 3. URL-encoded Arabic movie pattern (%d9%81%d9%8a%d9%84%d9%85)
    if REGEX_PATTERNS['movie'].search(url):
        return scrape_movie(url)
    else:
        return scrape_series(url)

def cleanup():
    """Cleanup resources before exit"""
    try:
        SESSION.close()
        log("Resources cleaned up", level="debug")
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")

if __name__ == "__main__":
    print_banner()
    start_time = time.time()
    result = None
    
    try:
        import sys
        if len(sys.argv) > 1:
            url_input = sys.argv[1]
        else:
            try:
                url_input = input(f"{Colors.CYAN}🔗 Enter URL to test (series, anime, or movie): {Colors.RESET}").strip()
            except EOFError:
                log("No input provided", level="error")
                sys.exit(1)
        
        if url_input:
            log("🚀 Starting extraction...", level="info")
            result = run_single(url_input)
            if result:
                os.makedirs('data', exist_ok=True)
                with open('data/test_output.json', 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                
                # Enhanced summary with Rich
                summary_table = Table(show_header=True, header_style="bold blue")
                summary_table.add_column("Field", style="dim")
                summary_table.add_column("Value")
                
                summary_table.add_row("Title", result.get("title", "Unknown"))
                summary_table.add_row("Type", result.get("type", "Unknown"))
                
                if result.get("type") != "movie":
                    seasons_count = len(result.get("seasons", []))
                    total_eps = sum(len(s.get("episodes", [])) for s in result.get("seasons", []))
                    summary_table.add_row("Seasons", str(seasons_count))
                    summary_table.add_row("Episodes", str(total_eps))
                else:
                    summary_table.add_row("Servers", str(len(result.get("streaming_servers", []))))
                
                elapsed = time.time() - start_time
                summary_table.add_row("Time", f"{elapsed:.2f}s")
                
                console.print("\n[bold]Summary:[/bold]")
                console.print(summary_table)
            
            # Final status
            status_style = "bold green" if result else "bold red"
            status_msg = "✓ Extraction completed" if result else "✗ Extraction failed"
            console.print(f"[{time.strftime('%H:%M:%S')}] {status_msg}", style=status_style)
        
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        log(f"🏁 Execution completed in {time_str}", level="success")
    
    except KeyboardInterrupt:
        log("\n⚠️  Interrupted by user", level="warning")
        sys.exit(130)
    except Exception as e:
        log(f"Fatal error: {str(e)}", level="error")
        logger.exception("Detailed traceback:")
        sys.exit(1)
    finally:
        cleanup()
