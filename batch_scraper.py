#!/usr/bin/env python3
"""
Advanced Batch Scraper with SQLite Database
- Real-time storage
- Resume capability
- Interrupt protection
- Multi-threaded for speed
- Cross-platform (Windows/Linux)
"""

import json
import sqlite3
import time
import signal
import sys
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Import scraping functions from scrape_single_test
from scrape_single_test import run_single, cleanup

# Configuration
DB_PATH = "data/topcinema.db"
MAX_WORKERS = 10  # Parallel workers
BATCH_SIZE = 100  # Commit every N items
LOG_FILE = "data/scraper.log"

# Thread-safe database lock
db_lock = Lock()

# Graceful shutdown flag
shutdown_flag = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Database:
    """Thread-safe SQLite database manager"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get database connection"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Initialize database schema"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Media table (movies/series)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                type TEXT,
                year INTEGER,
                imdb_rating REAL,
                poster TEXT,
                synopsis TEXT,
                trailer TEXT,
                status TEXT DEFAULT 'pending',
                error TEXT,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_id INTEGER,
                key TEXT,
                value TEXT,
                FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE
            )
        ''')
        
        # Seasons table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seasons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_id INTEGER,
                season_number INTEGER,
                poster TEXT,
                FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
                UNIQUE(media_id, season_number)
            )
        ''')
        
        # Episodes table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id INTEGER,
                episode_number INTEGER,
                FOREIGN KEY (season_id) REFERENCES seasons(id) ON DELETE CASCADE,
                UNIQUE(season_id, episode_number)
            )
        ''')
        
        # Servers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                episode_id INTEGER,
                server_number INTEGER,
                embed_url TEXT,
                FOREIGN KEY (episode_id) REFERENCES episodes(id) ON DELETE CASCADE
            )
        ''')
        
        # Create indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_media_url ON media(url)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_media_status ON media(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_metadata_media ON metadata(media_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_seasons_media ON seasons(media_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_episodes_season ON episodes(season_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_servers_episode ON servers(episode_id)')
        
        conn.commit()
        conn.close()
        
        logger.info(f"Database initialized: {self.db_path}")
    
    def add_urls(self, urls: List[str]):
        """Add URLs to database if not exists"""
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            for url in urls:
                cursor.execute(
                    'INSERT OR IGNORE INTO media (url, status) VALUES (?, ?)',
                    (url, 'pending')
                )
            
            conn.commit()
            conn.close()
    
    def get_pending_urls(self, limit: Optional[int] = None) -> List[tuple]:
        """Get pending URLs to scrape"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = "SELECT id, url FROM media WHERE status = 'pending'"
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        return [(row[0], row[1]) for row in results]
    
    def save_media(self, media_id: int, data: Dict):
        """Save scraped media data"""
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            try:
                # Update media table
                cursor.execute('''
                    UPDATE media SET
                        title = ?,
                        type = ?,
                        year = ?,
                        imdb_rating = ?,
                        poster = ?,
                        synopsis = ?,
                        trailer = ?,
                        status = 'completed',
                        scraped_at = ?
                    WHERE id = ?
                ''', (
                    data.get('title'),
                    data.get('type'),
                    data.get('year'),
                    data.get('imdb_rating'),
                    data.get('poster'),
                    data.get('synopsis'),
                    data.get('trailer'),
                    datetime.now(),
                    media_id
                ))
                
                # Save metadata
                if data.get('metadata'):
                    for key, value in data['metadata'].items():
                        if isinstance(value, list):
                            value = json.dumps(value, ensure_ascii=False)
                        cursor.execute(
                            'INSERT INTO metadata (media_id, key, value) VALUES (?, ?, ?)',
                            (media_id, key, str(value))
                        )
                
                # Save seasons and episodes
                if data.get('seasons'):
                    for season in data['seasons']:
                        cursor.execute(
                            'INSERT INTO seasons (media_id, season_number, poster) VALUES (?, ?, ?)',
                            (media_id, season['season_number'], season.get('poster'))
                        )
                        season_id = cursor.lastrowid
                        
                        # Save episodes
                        if season.get('episodes'):
                            for episode in season['episodes']:
                                cursor.execute(
                                    'INSERT INTO episodes (season_id, episode_number) VALUES (?, ?)',
                                    (season_id, episode['episode_number'])
                                )
                                episode_id = cursor.lastrowid
                                
                                # Save servers
                                if episode.get('servers'):
                                    for server in episode['servers']:
                                        cursor.execute(
                                            'INSERT INTO servers (episode_id, server_number, embed_url) VALUES (?, ?, ?)',
                                            (episode_id, server['server_number'], server['embed_url'])
                                        )
                
                # Save movie servers directly
                if data.get('servers'):
                    # For movies, create a dummy season/episode
                    cursor.execute(
                        'INSERT INTO seasons (media_id, season_number) VALUES (?, ?)',
                        (media_id, 1)
                    )
                    season_id = cursor.lastrowid
                    
                    cursor.execute(
                        'INSERT INTO episodes (season_id, episode_number) VALUES (?, ?)',
                        (season_id, 1)
                    )
                    episode_id = cursor.lastrowid
                    
                    for server in data['servers']:
                        cursor.execute(
                            'INSERT INTO servers (episode_id, server_number, embed_url) VALUES (?, ?, ?)',
                            (episode_id, server['server_number'], server['embed_url'])
                        )
                
                conn.commit()
                logger.info(f"✓ Saved: {data.get('title', 'Unknown')}")
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Error saving media {media_id}: {e}")
                cursor.execute(
                    'UPDATE media SET status = ?, error = ? WHERE id = ?',
                    ('error', str(e), media_id)
                )
                conn.commit()
            
            finally:
                conn.close()
    
    def mark_error(self, media_id: int, error: str):
        """Mark media as error"""
        with db_lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE media SET status = ?, error = ? WHERE id = ?',
                ('error', error, media_id)
            )
            conn.commit()
            conn.close()
    
    def get_stats(self) -> Dict:
        """Get scraping statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM media')
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE status = 'completed'")
        completed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE status = 'pending'")
        pending = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE status = 'error'")
        errors = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total': total,
            'completed': completed,
            'pending': pending,
            'errors': errors,
            'progress': (completed / total * 100) if total > 0 else 0
        }

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_flag
    logger.warning("\n⚠️  Interrupt received! Finishing current tasks...")
    shutdown_flag = True

def scrape_url(db: Database, media_id: int, url: str) -> bool:
    """Scrape a single URL and save to database"""
    try:
        logger.info(f"Scraping: {url}")
        data = run_single(url)
        
        if data:
            db.save_media(media_id, data)
            return True
        else:
            db.mark_error(media_id, "No data returned")
            return False
            
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        db.mark_error(media_id, str(e))
        return False
    finally:
        cleanup()

def load_urls_from_json(file_path: str) -> List[str]:
    """Load URLs from JSON file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data.get('urls', [])

def main():
    """Main scraping function"""
    global shutdown_flag
    
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("🚀 Starting Advanced Batch Scraper")
    
    # Initialize database
    db = Database(DB_PATH)
    
    # Load URLs from JSON files
    logger.info("Loading URLs...")
    movies = load_urls_from_json('data/movies.json')
    series = load_urls_from_json('data/series_animes.json')
    
    all_urls = movies + series
    logger.info(f"Found {len(movies)} movies and {len(series)} series/animes")
    
    # Add URLs to database
    logger.info("Adding URLs to database...")
    db.add_urls(all_urls)
    
    # Get initial stats
    stats = db.get_stats()
    logger.info(f"Database stats: {stats['completed']}/{stats['total']} completed ({stats['progress']:.1f}%)")
    
    if stats['pending'] == 0:
        logger.info("✅ All URLs already scraped!")
        return
    
    # Start scraping
    logger.info(f"Starting scraping with {MAX_WORKERS} workers...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while not shutdown_flag:
            # Get pending URLs
            pending = db.get_pending_urls(limit=MAX_WORKERS * 2)
            
            if not pending:
                break
            
            # Submit tasks
            futures = {
                executor.submit(scrape_url, db, media_id, url): (media_id, url)
                for media_id, url in pending
            }
            
            # Process completed tasks
            for future in as_completed(futures):
                if shutdown_flag:
                    break
                
                media_id, url = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Task failed for {url}: {e}")
            
            # Show progress
            stats = db.get_stats()
            elapsed = time.time() - start_time
            rate = stats['completed'] / elapsed if elapsed > 0 else 0
            eta = stats['pending'] / rate if rate > 0 else 0
            
            logger.info(f"Progress: {stats['completed']}/{stats['total']} ({stats['progress']:.1f}%) | "
                       f"Rate: {rate:.2f}/s | ETA: {eta/60:.1f}m")
    
    # Final stats
    elapsed = time.time() - start_time
    stats = db.get_stats()
    
    logger.info("\n" + "="*60)
    logger.info("🏁 Scraping completed!")
    logger.info(f"✅ Completed: {stats['completed']}")
    logger.info(f"❌ Errors: {stats['errors']}")
    logger.info(f"⏱️  Time: {elapsed/60:.1f} minutes ({elapsed/3600:.2f} hours)")
    logger.info(f"📊 Rate: {stats['completed']/elapsed:.2f} items/second")
    logger.info(f"💾 Database: {DB_PATH}")
    logger.info("="*60)

if __name__ == '__main__':
    main()
