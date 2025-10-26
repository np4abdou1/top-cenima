import sqlite3
import os

DB_PATH = "anime_db.sqlite"

def init_database():
    """Initialize SQLite database with schema"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Titles table (anime, series, movies)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS titles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL UNIQUE,
        type TEXT CHECK(type IN ('anime', 'series', 'movie')) NOT NULL,
        url TEXT UNIQUE,
        imdb_rating REAL,
        poster TEXT,
        story TEXT,
        year INTEGER,
        cast TEXT,
        trailer_url TEXT,
        details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Seasons table (for anime/series)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS seasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title_id INTEGER NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
        season_number INTEGER NOT NULL,
        title TEXT,
        poster TEXT,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(title_id, season_number)
    )
    """)
    
    # Episodes table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS episodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title_id INTEGER NOT NULL REFERENCES titles(id) ON DELETE CASCADE,
        season_id INTEGER REFERENCES seasons(id) ON DELETE CASCADE,
        episode_number INTEGER,
        title TEXT,
        poster TEXT,
        url TEXT,
        is_movie BOOLEAN DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(title_id, season_id, episode_number)
    )
    """)
    
    # Servers table (streaming sources)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS servers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        episode_id INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
        server_number INTEGER,
        embed_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(episode_id, server_number)
    )
    """)
    
    # Genres table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS genres (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    )
    """)
    
    # Title-Genres junction table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS title_genres (
        title_id INTEGER REFERENCES titles(id) ON DELETE CASCADE,
        genre_id INTEGER REFERENCES genres(id) ON DELETE CASCADE,
        PRIMARY KEY (title_id, genre_id)
    )
    """)
    
    # Create indexes for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_titles_type ON titles(type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_seasons_title_id ON seasons(title_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_title_id ON episodes(title_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_episodes_season_id ON episodes(season_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_servers_episode_id ON servers(episode_id)")
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

if __name__ == "__main__":
    init_database()
