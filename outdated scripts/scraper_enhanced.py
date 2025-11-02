"""
Enhanced TopCinema scraper with web dashboard and improved error handling
Features: Single-line progress, web status server, failed URLs export, retry logic
"""
import json
import os
import re
import time
import sqlite3
import sys
from typing import List, Dict, Optional
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging

import requests
from bs4 import BeautifulSoup

# Suppress warnings
logging.basicConfig(level=logging.ERROR)
requests.packages.urllib3.disable_warnings()

# Global stats for web dashboard
STATS = {
    'current_file': '',
    'current_url': '',
    'total_urls': 0,
    'completed': 0,
    'failed': 0,
    'success': 0,
    'start_time': time.time(),
    'failed_urls': [],
    'current_show': '',
    'episodes_found': 0,
    'servers_found': 0
}

class StatusHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress server logs
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.send_header('Refresh', '2')  # Auto-refresh every 2 seconds
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        
        elapsed = time.time() - STATS['start_time']
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        progress_pct = (STATS['completed'] / STATS['total_urls'] * 100) if STATS['total_urls'] > 0 else 0
        success_rate = (STATS['success'] / STATS['completed'] * 100) if STATS['completed'] > 0 else 0
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>TopCinema Scraper Status</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    margin: 0;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: rgba(255,255,255,0.1);
                    backdrop-filter: blur(10px);
                    border-radius: 20px;
                    padding: 30px;
                    box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
                }}
                h1 {{
                    text-align: center;
                    margin-bottom: 30px;
                    font-size: 2.5em;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                }}
                .stats-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .stat-card {{
                    background: rgba(255,255,255,0.2);
                    padding: 20px;
                    border-radius: 15px;
                    text-align: center;
                }}
                .stat-value {{
                    font-size: 2.5em;
                    font-weight: bold;
                    margin: 10px 0;
                }}
                .stat-label {{
                    font-size: 0.9em;
                    opacity: 0.9;
                }}
                .progress-bar {{
                    width: 100%;
                    height: 40px;
                    background: rgba(255,255,255,0.2);
                    border-radius: 20px;
                    overflow: hidden;
                    margin: 20px 0;
                }}
                .progress-fill {{
                    height: 100%;
                    background: linear-gradient(90deg, #00d2ff 0%, #3a47d5 100%);
                    transition: width 0.3s ease;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-weight: bold;
                }}
                .current-info {{
                    background: rgba(255,255,255,0.15);
                    padding: 20px;
                    border-radius: 15px;
                    margin-top: 20px;
                }}
                .current-info h3 {{
                    margin-top: 0;
                    border-bottom: 2px solid rgba(255,255,255,0.3);
                    padding-bottom: 10px;
                }}
                .info-row {{
                    margin: 10px 0;
                    padding: 8px;
                    background: rgba(0,0,0,0.2);
                    border-radius: 8px;
                }}
                .failed-list {{
                    max-height: 200px;
                    overflow-y: auto;
                    background: rgba(0,0,0,0.2);
                    padding: 15px;
                    border-radius: 10px;
                    margin-top: 10px;
                }}
                .failed-item {{
                    padding: 5px;
                    margin: 5px 0;
                    background: rgba(255,0,0,0.2);
                    border-left: 3px solid #ff4444;
                    border-radius: 5px;
                    font-size: 0.9em;
                }}
                .success {{ color: #4ade80; }}
                .warning {{ color: #fbbf24; }}
                .error {{ color: #f87171; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🎬 TopCinema Scraper Dashboard</h1>
                
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">Total URLs</div>
                        <div class="stat-value">{STATS['total_urls']}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Completed</div>
                        <div class="stat-value success">{STATS['completed']}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Success</div>
                        <div class="stat-value success">{STATS['success']}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Failed</div>
                        <div class="stat-value error">{STATS['failed']}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Success Rate</div>
                        <div class="stat-value">{success_rate:.1f}%</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Elapsed Time</div>
                        <div class="stat-value">{hours:02d}:{minutes:02d}:{seconds:02d}</div>
                    </div>
                </div>
                
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {progress_pct}%">
                        {progress_pct:.1f}%
                    </div>
                </div>
                
                <div class="current-info">
                    <h3>📂 Current File</h3>
                    <div class="info-row">{STATS['current_file']}</div>
                    
                    <h3>🔗 Current URL</h3>
                    <div class="info-row" style="word-break: break-all; font-size: 0.85em;">
                        {STATS['current_url']}
                    </div>
                    
                    <h3>🎭 Current Show</h3>
                    <div class="info-row">{STATS['current_show']}</div>
                    <div class="info-row">
                        📺 Episodes: {STATS['episodes_found']} | 🖥️ Servers: {STATS['servers_found']}
                    </div>
                </div>
                
                {f'''
                <div class="current-info">
                    <h3 class="error">❌ Failed URLs ({len(STATS['failed_urls'])})</h3>
                    <div class="failed-list">
                        {''.join([f'<div class="failed-item">{url}</div>' for url in STATS['failed_urls'][-20:]])}
                    </div>
                </div>
                ''' if STATS['failed_urls'] else ''}
            </div>
        </body>
        </html>
        """
        
        self.wfile.write(html.encode('utf-8'))

def start_web_server():
    """Start web server in background thread"""
    server = HTTPServer(('0.0.0.0', 8080), StatusHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server

# Import the rest from original scraper
from scraper_with_db import (
    Database, SESSION, REGEX_PATTERNS, fetch_html, extract_number_from_text,
    get_trailer_embed_url, scrape_season_episodes, extract_media_details,
    scrape_movie, clean_title
)

db = Database()

def scrape_series_enhanced(url: str, force_type: str = "series") -> Optional[Dict]:
    """Enhanced series scraper with better error handling"""
    try:
        soup = fetch_html(url)
        if not soup:
            return None
        
        details = extract_media_details(soup, force_type)
        STATS['current_show'] = details.get('title', 'Unknown')
        
        seasons: List[Dict] = []
        season_urls: Dict[int, str] = {}
        seen_urls = set()
        
        # Find seasons
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
            
            s_poster = None
            season_soup = fetch_html(s_url)
            if season_soup:
                poster_img = season_soup.select_one('.MainSingle .left .image img')
                if poster_img:
                    s_poster = poster_img.get('src') or poster_img.get('data-src')
            
            season_urls[s_num] = s_url
            seasons.append({
                "season_number": s_num,
                "poster": s_poster,
                "episodes": []
            })

        if not seasons:
            for a_el in soup.find_all('a', href=True):
                href = a_el['href']
                if '/series/' in href and 'الموسم' in href:
                    if href in seen_urls:
                        continue
                    seen_urls.add(href)
                    s_title = a_el.get('title') or a_el.get_text(strip=True) or ""
                    s_num = extract_number_from_text(s_title) or extract_number_from_text(href) or 1
                    
                    s_poster = None
                    season_soup = fetch_html(href)
                    if season_soup:
                        poster_img = season_soup.select_one('.MainSingle .left .image img')
                        if poster_img:
                            s_poster = poster_img.get('src') or poster_img.get('data-src')
                    
                    season_urls[s_num] = href
                    seasons.append({
                        "season_number": s_num,
                        "poster": s_poster,
                        "episodes": []
                    })

        seasons.sort(key=lambda s: s.get('season_number', 0))

        if not seasons:
            season_urls[1] = url
            seasons.append({
                "season_number": 1,
                "poster": details["poster"],
                "episodes": []
            })

        # Scrape episodes
        total_episodes = 0
        total_servers = 0
        for season in seasons:
            s_num = season["season_number"]
            if s_num in season_urls:
                season["episodes"] = scrape_season_episodes(season_urls[s_num])
                total_episodes += len(season["episodes"])
                for ep in season["episodes"]:
                    total_servers += len(ep.get("servers", []))
        
        STATS['episodes_found'] = total_episodes
        STATS['servers_found'] = total_servers

        # Get trailer
        episode_page_url = None
        if seasons:
            first_season_url = list(seen_urls)[0] if seen_urls else None
            if first_season_url:
                temp_soup = fetch_html(first_season_url)
                if temp_soup:
                    first_ep_link = temp_soup.select_one(".allepcont .row > a")
                    if first_ep_link:
                        episode_page_url = first_ep_link.get("href")
        
        trailer_url = None
        if episode_page_url:
            trailer_url = get_trailer_embed_url(url, episode_page_url)

        return {
            "title": details["title"],
            "type": force_type,
            "imdb_rating": details["imdb_rating"],
            "poster": details["poster"],
            "synopsis": details["synopsis"],
            "metadata": details["metadata"],
            "trailer": trailer_url,
            "year": None,
            "source_url": url,
            "seasons": seasons
        }
    except Exception as e:
        print(f"\n❌ Error scraping {url}: {str(e)}")
        return None

def run_single_enhanced(url: str, force_type: str = "series", max_retries: int = 3) -> Optional[Dict]:
    """Run single URL with retry logic"""
    for attempt in range(max_retries):
        try:
            if REGEX_PATTERNS['movie'].search(url):
                return scrape_movie(url)
            else:
                return scrape_series_enhanced(url, force_type=force_type)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                print(f"\n❌ Failed after {max_retries} attempts: {url}")
                return None
    return None

def process_json_file(json_file: str, force_type: str = "series"):
    """Process a single JSON file"""
    if not os.path.exists(json_file):
        print(f"⚠️  {json_file} not found, skipping...")
        return
    
    STATS['current_file'] = json_file
    print(f"\n{'='*80}")
    print(f"📂 Processing: {json_file}")
    print(f"{'='*80}\n")
    
    # Get pending URLs
    urls = db.get_pending_urls(json_file)
    
    if not urls:
        print(f"✅ All URLs from {json_file} already scraped!")
        return
    
    STATS['total_urls'] += len(urls)
    print(f"📊 Found {len(urls)} pending URLs\n")
    
    # Initialize progress
    db.init_progress(urls)
    
    # Process each URL
    for idx, url in enumerate(urls, 1):
        STATS['current_url'] = url
        
        # Single-line progress update
        progress = f"[{STATS['completed']}/{STATS['total_urls']}] {json_file.split('/')[-1]} | ✅ {STATS['success']} | ❌ {STATS['failed']} | {url[:60]}..."
        print(f"\r{progress}", end='', flush=True)
        
        try:
            result = run_single_enhanced(url, force_type=force_type)
            
            if result:
                show_id = db.insert_show(result)
                if show_id:
                    if result.get("type") in ["series", "anime"]:
                        db.insert_seasons_and_episodes(show_id, result.get("seasons", []))
                    else:
                        db.insert_movie_servers(show_id, result.get("streaming_servers", []))
                    
                    db.mark_progress(url, "completed", show_id)
                    STATS['success'] += 1
                else:
                    db.mark_progress(url, "failed", error="Duplicate show")
                    STATS['failed'] += 1
                    STATS['failed_urls'].append(url)
            else:
                db.mark_progress(url, "failed", error="Scraping returned no data")
                STATS['failed'] += 1
                STATS['failed_urls'].append(url)
        except Exception as e:
            db.mark_progress(url, "failed", error=str(e))
            STATS['failed'] += 1
            STATS['failed_urls'].append(url)
        
        STATS['completed'] += 1
    
    print()  # New line after progress

def main():
    """Main execution"""
    # Start web server
    print("🌐 Starting web dashboard on http://localhost:8080")
    server = start_web_server()
    print("✅ Dashboard ready!\n")
    
    # Process both JSON files
    json_files = [
        ("data/series_animes.json", "series"),
        ("data/movies.json", "movie")
    ]
    
    STATS['start_time'] = time.time()
    
    # Count total URLs first
    for json_file, _ in json_files:
        if os.path.exists(json_file):
            urls = db.get_pending_urls(json_file)
            STATS['total_urls'] += len(urls)
    
    # Process files
    for json_file, force_type in json_files:
        process_json_file(json_file, force_type)
    
    # Export failed URLs
    if STATS['failed_urls']:
        failed_file = f"data/failed_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump({
                'failed_count': len(STATS['failed_urls']),
                'timestamp': datetime.now().isoformat(),
                'urls': STATS['failed_urls']
            }, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Failed URLs exported to: {failed_file}")
    
    # Final summary
    elapsed = time.time() - STATS['start_time']
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)
    
    print(f"\n{'='*80}")
    print("🎉 SCRAPING COMPLETE!")
    print(f"{'='*80}")
    print(f"✅ Success: {STATS['success']}")
    print(f"❌ Failed: {STATS['failed']}")
    print(f"⏱️  Time: {hours:02d}:{minutes:02d}:{seconds:02d}")
    print(f"🌐 Dashboard: http://localhost:8080")
    print(f"{'='*80}\n")
    
    # Keep server running
    try:
        print("Press Ctrl+C to stop the server and exit...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down...")
        server.shutdown()

if __name__ == "__main__":
    main()
