"""
Clean 4-table database design for website
Tables: shows, seasons, episodes, servers + progress tracking
"""
import sqlite3
import os

def init_database(db_path: str = "data/scraper.db"):
    """Create 4-table database schema with progress tracking"""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # ============================================
    # TABLE 1: SHOWS (Main content)
    # ============================================
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
    
    # Essential indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_type ON shows(type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_slug ON shows(slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_year ON shows(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_rating ON shows(imdb_rating DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shows_created ON shows(created_at DESC)")
    
    # Full-text search
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS shows_fts USING fts5(
        title, 
        synopsis,
        genres,
        cast,
        content='shows',
        content_rowid='id'
    )
    """)
    
    # Auto-sync FTS
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS shows_fts_insert AFTER INSERT ON shows BEGIN
        INSERT INTO shows_fts(rowid, title, synopsis, genres, cast) 
        VALUES (new.id, new.title, new.synopsis, new.genres, new.cast);
    END
    """)
    
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS shows_fts_delete AFTER DELETE ON shows BEGIN
        DELETE FROM shows_fts WHERE rowid = old.id;
    END
    """)
    
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS shows_fts_update AFTER UPDATE ON shows BEGIN
        UPDATE shows_fts SET title = new.title, synopsis = new.synopsis, genres = new.genres, cast = new.cast
        WHERE rowid = new.id;
    END
    """)
    
    # ============================================
    # TABLE 2: SEASONS (For series/anime)
    # ============================================
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
    
    # ============================================
    # TABLE 3: EPISODES (For series/anime)
    # ============================================
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
    
    # ============================================
    # TABLE 4: SERVERS (Streaming links)
    # ============================================
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
    
    # ============================================
    # PROGRESS TABLE (Track scraping)
    # ============================================
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
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_url ON scrape_progress(url)")
    
    conn.commit()
    print(f"✓ Clean 4-table database initialized at {db_path}")
    print(f"✓ Tables: shows, seasons, episodes, servers + progress tracking")
    conn.close()

if __name__ == "__main__":
    init_database()
