#!/usr/bin/env python3
"""
Extract Redflag Shows to JSON
Extracts all shows/episodes with issues and saves their source URLs to a JSON file
"""

import sqlite3
import json
from datetime import datetime

DB_PATH = "data/scrapped.db"
OUTPUT_FILE = "data/redflag_shows.json"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def extract_redflag_shows():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("🚩 EXTRACTING REDFLAG SHOWS TO JSON")
    print("=" * 80)
    
    redflag_data = {
        "generated_at": datetime.now().isoformat(),
        "total_count": 0,
        "categories": {}
    }
    
    # === 1. Failed Scrapes ===
    print("\n1. Extracting failed scrapes...")
    cursor.execute("""
        SELECT url, error_message 
        FROM scrape_progress 
        WHERE status = 'failed'
        ORDER BY url
    """)
    
    failed = cursor.fetchall()
    redflag_data["categories"]["failed_scrapes"] = {
        "count": len(failed),
        "description": "URLs that failed to scrape",
        "urls": [{"url": row['url'], "error": row['error_message']} for row in failed]
    }
    print(f"   Found {len(failed)} failed scrapes")
    
    # === 2. Movies without servers ===
    print("\n2. Extracting movies without servers...")
    cursor.execute("""
        SELECT s.id, s.title, s.source_url
        FROM shows s
        WHERE s.type = 'movie'
        AND s.id NOT IN (
            SELECT DISTINCT parent_id FROM servers WHERE parent_type = 'movie'
        )
        ORDER BY s.title
    """)
    
    movies_no_servers = cursor.fetchall()
    redflag_data["categories"]["movies_without_servers"] = {
        "count": len(movies_no_servers),
        "description": "Movies that have no streaming servers",
        "urls": [{"id": row['id'], "title": row['title'], "url": row['source_url']} for row in movies_no_servers]
    }
    print(f"   Found {len(movies_no_servers)} movies without servers")
    
    # === 3. Series/Anime without seasons ===
    print("\n3. Extracting series/anime without seasons...")
    cursor.execute("""
        SELECT s.id, s.title, s.type, s.source_url
        FROM shows s
        WHERE s.type IN ('series', 'anime')
        AND s.id NOT IN (SELECT DISTINCT show_id FROM seasons)
        ORDER BY s.type, s.title
    """)
    
    shows_no_seasons = cursor.fetchall()
    redflag_data["categories"]["shows_without_seasons"] = {
        "count": len(shows_no_seasons),
        "description": "Series/Anime with no seasons",
        "urls": [{"id": row['id'], "title": row['title'], "type": row['type'], "url": row['source_url']} for row in shows_no_seasons]
    }
    print(f"   Found {len(shows_no_seasons)} shows without seasons")
    
    # === 4. Seasons without episodes ===
    print("\n4. Extracting seasons without episodes...")
    cursor.execute("""
        SELECT s.id, sh.id as show_id, sh.title, sh.type, s.season_number, sh.source_url
        FROM seasons s
        JOIN shows sh ON s.show_id = sh.id
        WHERE s.id NOT IN (SELECT DISTINCT season_id FROM episodes)
        ORDER BY sh.title, s.season_number
    """)
    
    seasons_no_episodes = cursor.fetchall()
    redflag_data["categories"]["seasons_without_episodes"] = {
        "count": len(seasons_no_episodes),
        "description": "Seasons with no episodes",
        "urls": [{"show_id": row['show_id'], "season_id": row['id'], "title": row['title'], 
                  "type": row['type'], "season_number": row['season_number'], "url": row['source_url']} 
                 for row in seasons_no_episodes]
    }
    print(f"   Found {len(seasons_no_episodes)} seasons without episodes")
    
    # === 5. Episodes without servers ===
    print("\n5. Extracting episodes without servers...")
    cursor.execute("""
        SELECT e.id, sh.id as show_id, sh.title, sh.type, se.season_number, e.episode_number, sh.source_url
        FROM episodes e
        JOIN seasons se ON e.season_id = se.id
        JOIN shows sh ON se.show_id = sh.id
        WHERE e.id NOT IN (
            SELECT DISTINCT parent_id FROM servers WHERE parent_type = 'episode'
        )
        ORDER BY sh.title, se.season_number, e.episode_number
    """)
    
    episodes_no_servers = cursor.fetchall()
    redflag_data["categories"]["episodes_without_servers"] = {
        "count": len(episodes_no_servers),
        "description": "Episodes with no streaming servers",
        "urls": [{"show_id": row['show_id'], "episode_id": row['id'], "title": row['title'], 
                  "type": row['type'], "season": row['season_number'], 
                  "episode": row['episode_number'], "url": row['source_url']} 
                 for row in episodes_no_servers]
    }
    print(f"   Found {len(episodes_no_servers)} episodes without servers")
    
    # === 6. Shows missing Season 1 ===
    # NOTE: This is no longer considered a redflag - sequels and continuations naturally don't have Season 1
    print("\n6. Skipping shows missing Season 1 (no longer considered a redflag)...")
    
    redflag_data["categories"]["shows_missing_season_1"] = {
        "count": 0,
        "description": "Shows that don't have Season 1 (sequels/continuations - not a redflag)",
        "urls": []
    }
    print(f"   Skipped - not a redflag")
    
    # === 7. Seasons not starting at Episode 1 ===
    # NOTE: This is now more lenient - continuation seasons naturally don't start at episode 1
    print("\n7. Extracting seasons not starting at Episode 1 (excluding Season 1 itself)...")
    cursor.execute("""
        SELECT 
            sh.id as show_id,
            sh.title,
            sh.type,
            sh.source_url,
            se.season_number,
            MIN(e.episode_number) as first_episode,
            MAX(e.episode_number) as last_episode,
            COUNT(e.id) as episode_count
        FROM shows sh
        JOIN seasons se ON sh.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE sh.type IN ('series', 'anime')
        AND se.season_number = 1
        GROUP BY se.id
        HAVING first_episode != 1
        ORDER BY sh.title, se.season_number
    """)
    
    wrong_start = cursor.fetchall()
    redflag_data["categories"]["seasons_not_starting_at_episode_1"] = {
        "count": len(wrong_start),
        "description": "Season 1 that starts at episode numbers other than 1 (may need re-scraping)",
        "urls": [{"show_id": row['show_id'], "title": row['title'], "type": row['type'],
                  "season": row['season_number'], "first_episode": row['first_episode'],
                  "last_episode": row['last_episode'], "episode_count": row['episode_count'],
                  "url": row['source_url']} 
                 for row in wrong_start]
    }
    print(f"   Found {len(wrong_start)} Season 1s not starting at Episode 1")
    
    # === 8. Seasons with episode gaps ===
    print("\n8. Extracting seasons with episode gaps...")
    cursor.execute("""
        SELECT DISTINCT
            sh.id as show_id,
            sh.title,
            sh.type,
            sh.source_url,
            se.id as season_id,
            se.season_number
        FROM shows sh
        JOIN seasons se ON sh.id = se.show_id
        JOIN episodes e ON se.id = e.season_id
        WHERE sh.type IN ('series', 'anime')
        ORDER BY sh.title, se.season_number
    """)
    
    all_seasons = cursor.fetchall()
    gap_seasons = []
    
    for season in all_seasons:
        cursor.execute("""
            SELECT episode_number 
            FROM episodes 
            WHERE season_id = ?
            ORDER BY episode_number
        """, (season['season_id'],))
        
        episodes = [e['episode_number'] for e in cursor.fetchall()]
        
        if episodes:
            # Check for gaps
            expected = list(range(min(episodes), max(episodes) + 1))
            missing = sorted(set(expected) - set(episodes))
            
            if missing:
                gap_seasons.append({
                    "show_id": season['show_id'],
                    "title": season['title'],
                    "type": season['type'],
                    "season": season['season_number'],
                    "missing_episodes": missing,
                    "episode_range": f"{min(episodes)}-{max(episodes)}",
                    "url": season['source_url']
                })
    
    redflag_data["categories"]["seasons_with_episode_gaps"] = {
        "count": len(gap_seasons),
        "description": "Seasons with missing episodes in the numbering sequence",
        "urls": gap_seasons
    }
    print(f"   Found {len(gap_seasons)} seasons with episode gaps")
    
    # === 9. Shows with season gaps ===
    print("\n9. Extracting shows with season gaps...")
    cursor.execute("""
        SELECT DISTINCT s.id, s.title, s.type, s.source_url
        FROM shows s
        JOIN seasons se ON s.id = se.show_id
        WHERE s.type IN ('series', 'anime')
        AND s.id IN (SELECT show_id FROM seasons)
        ORDER BY s.title
    """)
    
    all_shows = cursor.fetchall()
    season_gap_shows = []
    
    for show in all_shows:
        cursor.execute("""
            SELECT season_number 
            FROM seasons 
            WHERE show_id = ?
            ORDER BY season_number
        """, (show['id'],))
        
        seasons = [s['season_number'] for s in cursor.fetchall()]
        
        if seasons and len(seasons) > 1:
            # Check for gaps
            expected = list(range(min(seasons), max(seasons) + 1))
            missing = sorted(set(expected) - set(seasons))
            
            if missing:
                season_gap_shows.append({
                    "id": show['id'],
                    "title": show['title'],
                    "type": show['type'],
                    "has_seasons": seasons,
                    "missing_seasons": missing,
                    "url": show['source_url']
                })
    
    redflag_data["categories"]["shows_with_season_gaps"] = {
        "count": len(season_gap_shows),
        "description": "Shows with missing seasons in the numbering sequence",
        "urls": season_gap_shows
    }
    print(f"   Found {len(season_gap_shows)} shows with season gaps")
    
    # Calculate total
    total = sum(cat["count"] for cat in redflag_data["categories"].values())
    redflag_data["total_count"] = total
    
    # Save to JSON
    print(f"\n{'=' * 80}")
    print(f"💾 Saving to {OUTPUT_FILE}...")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(redflag_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Successfully saved {total} total redflag items across {len(redflag_data['categories'])} categories")
    print(f"{'=' * 80}")
    
    # Print summary
    print("\n📊 SUMMARY BY CATEGORY:")
    print("-" * 80)
    for category, data in redflag_data["categories"].items():
        print(f"  • {category}: {data['count']}")
    
    conn.close()

if __name__ == "__main__":
    try:
        extract_redflag_shows()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
