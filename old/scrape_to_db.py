import sqlite3
import json
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
import time
from datetime import datetime
import re

DB_PATH = "anime_db.sqlite"
TITLES_FILE = "data/titles.json"
MAX_WORKERS = 8
TIMEOUT = 15
MAX_RETRIES = 3
RETRY_DELAY = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

class DBConnection:
    """Thread-safe database connection pool"""
    def __init__(self, db_path):
        self.db_path = db_path
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

db_pool = DBConnection(DB_PATH)

def fetch_page(url, retries=MAX_RETRIES):
    """Fetch page with retry logic and better error handling"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                return response.text
            elif response.status_code == 429:  # Rate limited
                print(f"Rate limited, waiting {RETRY_DELAY * (attempt + 1)}s...")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"HTTP {response.status_code} for {url}")
        except requests.Timeout:
            print(f"Timeout on attempt {attempt + 1}/{retries} for {url}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
        except requests.ConnectionError as e:
            print(f"Connection error on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
    
    return None

def extract_title_details(html, url):
    """Extract title details from page"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        title = soup.find('h1', class_='post-title')
        if not title:
            title = soup.find('h1', class_='title')
        title_text = title.get_text(strip=True) if title else "Unknown"
        
        # Find poster image
        poster = soup.find('img', {'alt': True})
        poster_url = poster.get('src') if poster else None
        
        # Extract story
        story = soup.find('div', class_='story')
        story_text = story.get_text(strip=True) if story else None
        
        # Extract IMDb rating
        imdb_elem = soup.find('span', class_='imdbRating')
        if not imdb_elem:
            imdb_elem = soup.find('div', class_='imdbR')
        imdb_rating = None
        if imdb_elem:
            rating_text = imdb_elem.get_text(strip=True)
            try:
                imdb_rating = float(re.search(r'\d+\.?\d*', rating_text).group())
            except:
                pass
        
        # Determine type based on URL
        content_type = 'movie' if '/فيلم-' in url else ('anime' if '/انمي-' in url else 'series')
        
        return {
            'title': title_text,
            'type': content_type,
            'url': url,
            'poster': poster_url,
            'story': story_text,
            'imdb_rating': imdb_rating
        }
    except Exception as e:
        print(f"Error extracting details from {url}: {e}")
        return None

def extract_seasons_and_episodes(html, title_id, url):
    """Extract seasons and episodes from series/anime page"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        
        season_divs = soup.find_all('div', class_='season')
        
        for season_div in season_divs:
            season_title = season_div.find('h3', class_='season-title')
            season_num = 1
            
            if season_title:
                text = season_title.get_text(strip=True)
                season_num = parse_arabic_ordinal(text)
            
            cursor.execute("""
                INSERT OR IGNORE INTO seasons (title_id, season_number, title, url)
                VALUES (?, ?, ?, ?)
            """, (title_id, season_num, season_title.get_text(strip=True) if season_title else f"Season {season_num}", url))
            conn.commit()
            
            season_id = cursor.lastrowid
            
            episode_divs = season_div.find_all('div', class_='episode')
            
            for idx, ep_div in enumerate(episode_divs, 1):
                ep_title = ep_div.find('span', class_='ep-title')
                ep_poster = ep_div.find('img')
                
                cursor.execute("""
                    INSERT OR IGNORE INTO episodes (title_id, season_id, episode_number, title, poster)
                    VALUES (?, ?, ?, ?, ?)
                """, (title_id, season_id, idx, ep_title.get_text(strip=True) if ep_title else f"Episode {idx}", 
                      ep_poster.get('src') if ep_poster else None))
                conn.commit()
                
                episode_id = cursor.lastrowid
                
                server_links = ep_div.find_all('a', class_='server')
                for server_num, link in enumerate(server_links, 1):
                    embed_url = link.get('href')
                    if embed_url:
                        cursor.execute("""
                            INSERT OR IGNORE INTO servers (episode_id, server_number, embed_url)
                            VALUES (?, ?, ?)
                        """, (episode_id, server_num, embed_url))
                        conn.commit()
        
        conn.close()
        return True
    except Exception as e:
        print(f"Error extracting seasons/episodes: {e}")
        return False

def extract_movie_servers(html, title_id):
    """Extract servers for movie from watch page"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        
        # Insert movie as single episode
        cursor.execute("""
            INSERT OR IGNORE INTO episodes (title_id, episode_number, title, is_movie)
            VALUES (?, ?, ?, ?)
        """, (title_id, 1, "Movie", 1))
        conn.commit()
        
        episode_id = cursor.lastrowid
        
        # Extract servers from watch--servers--list
        server_items = soup.find_all('li', class_='server--item')
        
        for server_num, item in enumerate(server_items, 1):
            # Get server name from span
            span = item.find('span')
            server_name = span.get_text(strip=True) if span else f"Server {server_num}"
            
            # Get data attributes
            data_id = item.get('data-id')
            data_server = item.get('data-server')
            
            # Try to find embed URL from iframe or data attributes
            embed_url = None
            
            # First try to get from iframe in the page
            iframe = soup.find('iframe', {'src': True})
            if iframe and server_num == 1:  # First server is usually the default
                embed_url = iframe.get('src')
            
            # If no iframe found, construct URL from data attributes
            if not embed_url and data_id and data_server:
                embed_url = f"https://web7.topcinema.cam/Ajaxat/?action=getEpisodeServers&id={data_id}&server={data_server}"
            
            if embed_url:
                cursor.execute("""
                    INSERT OR IGNORE INTO servers (episode_id, server_number, embed_url)
                    VALUES (?, ?, ?)
                """, (episode_id, server_num, embed_url))
                conn.commit()
        
        conn.close()
        return True
    except Exception as e:
        print(f"Error extracting movie servers: {e}")
        return False

def parse_arabic_ordinal(text):
    """Parse Arabic ordinal numbers to integers"""
    ordinals = {
        'الاول': 1, 'الأول': 1,
        'الثاني': 2, 'الثانى': 2,
        'الثالث': 3, 'الثالثة': 3,
        'الرابع': 4, 'الرابعة': 4,
        'الخامس': 5, 'الخامسة': 5,
        'السادس': 6, 'السادسة': 6,
        'السابع': 7, 'السابعة': 7,
        'الثامن': 8, 'الثامنة': 8,
        'التاسع': 9, 'التاسعة': 9,
        'العاشر': 10, 'العاشرة': 10,
    }
    
    for key, value in ordinals.items():
        if key in text:
            return value
    
    # Try to extract number if present
    match = re.search(r'\d+', text)
    return int(match.group()) if match else 1

def scrape_url(url):
    """Scrape single URL and store in database"""
    print(f"Scraping: {url}")
    
    # Determine if it's a movie
    is_movie = '/فيلم-' in url
    
    # For movies, we need to fetch both the details page and watch page
    if is_movie:
        # Details page (without /watch/)
        details_url = url.rstrip('/watch/')
        details_html = fetch_page(details_url)
        if not details_html:
            return False
        
        # Watch page (with /watch/)
        watch_url = url if url.endswith('/watch/') else url.rstrip('/') + '/watch/'
        watch_html = fetch_page(watch_url)
        if not watch_html:
            return False
    else:
        html = fetch_page(url)
        if not html:
            return False
        details_html = html
        watch_html = None
    
    # Extract title details
    details = extract_title_details(details_html, url)
    if not details:
        return False
    
    # Insert title
    conn = db_pool.get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO titles (title, type, url, poster, story, imdb_rating)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (details['title'], details['type'], details['url'], details['poster'], 
              details['story'], details['imdb_rating']))
        conn.commit()
        
        title_id = cursor.lastrowid
        
        # Extract content based on type
        if is_movie and watch_html:
            extract_movie_servers(watch_html, title_id)
        else:
            extract_seasons_and_episodes(details_html, title_id, url)
        
        print(f"✓ Completed: {details['title']}")
        return True
    except Exception as e:
        print(f"Error inserting title: {e}")
        return False
    finally:
        conn.close()

def scrape_all_from_titles_json():
    """Scrape all URLs from titles.json concurrently"""
    try:
        with open(TITLES_FILE, 'r', encoding='utf-8') as f:
            titles = json.load(f)
        
        print(f"Found {len(titles)} URLs to scrape")
        
        completed = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(scrape_url, url): url for url in titles}
            
            for future in as_completed(futures):
                try:
                    if future.result():
                        completed += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"Task error: {e}")
                    failed += 1
                
                print(f"Progress: {completed + failed}/{len(titles)}")
        
        print(f"\nCompleted: {completed}, Failed: {failed}")
    except Exception as e:
        print(f"Error reading titles.json: {e}")

if __name__ == "__main__":
    scrape_all_from_titles_json()
