#!/usr/bin/env python3
"""
Check Show Details - Query a specific show from the database
Usage: python check_show.py "Show Name"
"""
import sqlite3
import sys

DB_PATH = "data/scrapped.db"

def check_show(show_name):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Find show by title (case-insensitive, partial match)
    cursor.execute("""
        SELECT id, title, type, source_url, imdb_rating, year
        FROM shows 
        WHERE LOWER(title) LIKE LOWER(?)
        LIMIT 10
    """, (f'%{show_name}%',))
    
    shows = cursor.fetchall()
    
    if not shows:
        print(f"❌ No shows found matching '{show_name}'")
        conn.close()
        return
    
    if len(shows) > 1:
        print(f"📋 Found {len(shows)} shows matching '{show_name}':\n")
        for i, show in enumerate(shows, 1):
            print(f"  {i}. {show['title']} ({show['type']}) - ID: {show['id']}")
        print("\n" + "=" * 80)
        
        choice = input("Enter number to see details (or press Enter to see all): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(shows):
            shows = [shows[int(choice) - 1]]
        print()
    
    for show in shows:
        show_id = show['id']
        
        print("=" * 80)
        print(f"🎬 {show['title']}")
        print("=" * 80)
        print(f"Type: {show['type']}")
        print(f"ID: {show_id}")
        print(f"IMDB: {show['imdb_rating'] or 'N/A'}")
        print(f"Year: {show['year'] or 'N/A'}")
        print(f"URL: {show['source_url']}")
        
        if show['type'] in ['series', 'anime']:
            # Get seasons
            cursor.execute("""
                SELECT season_number, COUNT(e.id) as episode_count
                FROM seasons s
                LEFT JOIN episodes e ON s.id = e.season_id
                WHERE s.show_id = ?
                GROUP BY s.season_number
                ORDER BY s.season_number
            """, (show_id,))
            
            seasons = cursor.fetchall()
            
            if seasons:
                print(f"\n📺 Seasons: {len(seasons)}")
                for season in seasons:
                    print(f"  Season {season['season_number']}: {season['episode_count']} episodes")
                    
                    # Show episode range
                    cursor.execute("""
                        SELECT MIN(episode_number) as first, MAX(episode_number) as last
                        FROM episodes
                        WHERE season_id = (
                            SELECT id FROM seasons WHERE show_id = ? AND season_number = ?
                        )
                    """, (show_id, season['season_number']))
                    
                    ep_range = cursor.fetchone()
                    if ep_range['first'] is not None:
                        print(f"    Range: Episode {ep_range['first']} - {ep_range['last']}")
                        
                        # Check for gaps
                        cursor.execute("""
                            SELECT episode_number
                            FROM episodes
                            WHERE season_id = (
                                SELECT id FROM seasons WHERE show_id = ? AND season_number = ?
                            )
                            ORDER BY episode_number
                        """, (show_id, season['season_number']))
                        
                        episodes = [row['episode_number'] for row in cursor.fetchall()]
                        if episodes:
                            expected = list(range(int(episodes[0]), int(episodes[-1]) + 1))
                            missing = [e for e in expected if e not in [int(ep) for ep in episodes]]
                            if missing:
                                print(f"    ⚠️  Missing episodes: {missing}")
                
                # Get server count
                cursor.execute("""
                    SELECT COUNT(DISTINCT s.id) as server_count
                    FROM servers s
                    JOIN episodes e ON s.parent_id = e.id AND s.parent_type = 'episode'
                    JOIN seasons se ON e.season_id = se.id
                    WHERE se.show_id = ?
                """, (show_id,))
                
                server_count = cursor.fetchone()['server_count']
                print(f"\n🖥️  Total Servers: {server_count}")
            else:
                print(f"\n⚠️  No seasons found")
        
        elif show['type'] == 'movie':
            # Get server count for movie
            cursor.execute("""
                SELECT COUNT(*) as server_count
                FROM servers
                WHERE parent_type = 'movie' AND parent_id = ?
            """, (show_id,))
            
            server_count = cursor.fetchone()['server_count']
            print(f"\n🖥️  Servers: {server_count}")
        
        print("=" * 80)
        print()
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_show.py \"Show Name\"")
        print("\nExamples:")
        print("  python check_show.py \"Black Clover\"")
        print("  python check_show.py \"Fullmetal Alchemist\"")
        print("  python check_show.py \"Detective Conan\"")
        sys.exit(1)
    
    show_name = " ".join(sys.argv[1:])
    check_show(show_name)
