#!/usr/bin/env python3
"""
Database Viewer and Query Tool
View and export scraped data from SQLite database
"""

import sqlite3
import json
import argparse
from typing import List, Dict
from pathlib import Path

DB_PATH = "data/topcinema.db"

class DatabaseViewer:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        cursor = self.conn.cursor()
        
        stats = {}
        
        # Media stats
        cursor.execute("SELECT COUNT(*) FROM media")
        stats['total_media'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE type = 'movie'")
        stats['movies'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE type = 'series'")
        stats['series'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE status = 'completed'")
        stats['completed'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE status = 'pending'")
        stats['pending'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM media WHERE status = 'error'")
        stats['errors'] = cursor.fetchone()[0]
        
        # Content stats
        cursor.execute("SELECT COUNT(*) FROM seasons")
        stats['total_seasons'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM episodes")
        stats['total_episodes'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM servers")
        stats['total_servers'] = cursor.fetchone()[0]
        
        return stats
    
    def search_media(self, query: str, limit: int = 10) -> List[Dict]:
        """Search media by title"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM media WHERE title LIKE ? LIMIT ?",
            (f"%{query}%", limit)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_media_full(self, media_id: int) -> Dict:
        """Get complete media data with seasons/episodes/servers"""
        cursor = self.conn.cursor()
        
        # Get media
        cursor.execute("SELECT * FROM media WHERE id = ?", (media_id,))
        media = dict(cursor.fetchone())
        
        # Get metadata
        cursor.execute("SELECT key, value FROM metadata WHERE media_id = ?", (media_id,))
        media['metadata'] = {row['key']: row['value'] for row in cursor.fetchall()}
        
        # Get seasons
        cursor.execute("SELECT * FROM seasons WHERE media_id = ? ORDER BY season_number", (media_id,))
        seasons = []
        
        for season_row in cursor.fetchall():
            season = dict(season_row)
            season_id = season['id']
            
            # Get episodes
            cursor.execute("SELECT * FROM episodes WHERE season_id = ? ORDER BY episode_number", (season_id,))
            episodes = []
            
            for episode_row in cursor.fetchall():
                episode = dict(episode_row)
                episode_id = episode['id']
                
                # Get servers
                cursor.execute("SELECT * FROM servers WHERE episode_id = ? ORDER BY server_number", (episode_id,))
                episode['servers'] = [dict(row) for row in cursor.fetchall()]
                
                episodes.append(episode)
            
            season['episodes'] = episodes
            seasons.append(season)
        
        media['seasons'] = seasons
        
        return media
    
    def export_to_json(self, output_file: str):
        """Export all completed media to JSON"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM media WHERE status = 'completed'")
        
        all_media = []
        for row in cursor.fetchall():
            media = self.get_media_full(row['id'])
            all_media.append(media)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_media, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Exported {len(all_media)} items to {output_file}")
    
    def list_errors(self) -> List[Dict]:
        """List all failed scrapes"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, url, error FROM media WHERE status = 'error'")
        return [dict(row) for row in cursor.fetchall()]
    
    def reset_errors(self):
        """Reset all errors to pending for retry"""
        cursor = self.conn.cursor()
        cursor.execute("UPDATE media SET status = 'pending', error = NULL WHERE status = 'error'")
        self.conn.commit()
        print(f"✅ Reset {cursor.rowcount} errors to pending")
    
    def close(self):
        self.conn.close()

def main():
    parser = argparse.ArgumentParser(description='TopCinema Database Viewer')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--search', type=str, help='Search media by title')
    parser.add_argument('--export', type=str, help='Export to JSON file')
    parser.add_argument('--errors', action='store_true', help='List all errors')
    parser.add_argument('--reset-errors', action='store_true', help='Reset errors to pending')
    parser.add_argument('--get', type=int, help='Get full media data by ID')
    
    args = parser.parse_args()
    
    viewer = DatabaseViewer()
    
    if args.stats:
        stats = viewer.get_stats()
        print("\n📊 Database Statistics")
        print("=" * 50)
        print(f"Total Media: {stats['total_media']}")
        print(f"  Movies: {stats['movies']}")
        print(f"  Series: {stats['series']}")
        print(f"\nStatus:")
        print(f"  Completed: {stats['completed']}")
        print(f"  Pending: {stats['pending']}")
        print(f"  Errors: {stats['errors']}")
        print(f"\nContent:")
        print(f"  Seasons: {stats['total_seasons']}")
        print(f"  Episodes: {stats['total_episodes']}")
        print(f"  Servers: {stats['total_servers']}")
        print("=" * 50)
    
    elif args.search:
        results = viewer.search_media(args.search)
        print(f"\n🔍 Search results for '{args.search}':")
        for media in results:
            print(f"  [{media['id']}] {media['title']} ({media['type']}) - {media['status']}")
    
    elif args.export:
        viewer.export_to_json(args.export)
    
    elif args.errors:
        errors = viewer.list_errors()
        print(f"\n❌ Errors ({len(errors)}):")
        for error in errors[:20]:
            print(f"  [{error['id']}] {error['url']}")
            print(f"      Error: {error['error']}")
    
    elif args.reset_errors:
        viewer.reset_errors()
    
    elif args.get:
        media = viewer.get_media_full(args.get)
        print(json.dumps(media, ensure_ascii=False, indent=2))
    
    else:
        parser.print_help()
    
    viewer.close()

if __name__ == '__main__':
    main()
