"""
Simple query interface for 3-table database
Features: search, filtering, trending, and statistics
"""
import sqlite3
import sys
from typing import List, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

class DatabaseQuery:
    def __init__(self, db_path: str = "data/scraper.db"):
        self.db_path = db_path
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def search_shows(self, query: str, limit: int = 20) -> List[dict]:
        """Full-text search for shows"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT s.id, s.title, s.slug, s.type, s.year, s.imdb_rating, s.poster, s.genres
        FROM shows_fts
        JOIN shows s ON shows_fts.rowid = s.id
        WHERE shows_fts MATCH ?
        ORDER BY s.imdb_rating DESC
        LIMIT ?
        """, (query, limit))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_shows_by_genre(self, genre: str, limit: int = 20) -> List[dict]:
        """Get shows by genre (searches in genres text field)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT id, title, slug, type, year, imdb_rating, genres
        FROM shows
        WHERE genres LIKE ?
        ORDER BY imdb_rating DESC
        LIMIT ?
        """, (f'%{genre}%', limit))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_trending_shows(self, limit: int = 10) -> List[dict]:
        """Get recently added shows"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT id, title, slug, type, year, imdb_rating, poster, genres
        FROM shows
        ORDER BY created_at DESC, imdb_rating DESC
        LIMIT ?
        """, (limit,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_top_rated(self, show_type: Optional[str] = None, limit: int = 10) -> List[dict]:
        """Get top rated shows"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if show_type:
            cursor.execute("""
            SELECT id, title, slug, type, year, imdb_rating
            FROM shows
            WHERE type = ? AND imdb_rating IS NOT NULL
            ORDER BY imdb_rating DESC
            LIMIT ?
            """, (show_type, limit))
        else:
            cursor.execute("""
            SELECT id, title, slug, type, year, imdb_rating
            FROM shows
            WHERE imdb_rating IS NOT NULL
            ORDER BY imdb_rating DESC
            LIMIT ?
            """, (limit,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_recent_shows(self, limit: int = 10) -> List[dict]:
        """Get recently added shows"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT id, title, slug, type, year, imdb_rating, created_at
        FROM shows
        ORDER BY created_at DESC
        LIMIT ?
        """, (limit,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results
    
    def get_show_details(self, show_id: int) -> Optional[dict]:
        """Get complete show details"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM shows WHERE id = ?", (show_id,))
        show = cursor.fetchone()
        
        if not show:
            conn.close()
            return None
        
        show_dict = dict(show)
        
        # Get seasons with episode counts
        cursor.execute("""
        SELECT s.season_number, s.poster, COUNT(e.id) as episode_count
        FROM seasons s
        LEFT JOIN episodes e ON s.id = e.season_id
        WHERE s.show_id = ?
        GROUP BY s.id, s.season_number, s.poster
        ORDER BY s.season_number
        """, (show_id,))
        show_dict['seasons'] = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return show_dict
    
    def get_all_genres(self) -> List[str]:
        """Get all unique genres from shows"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT genres FROM shows WHERE genres IS NOT NULL")
        all_genres = set()
        for row in cursor.fetchall():
            if row[0]:
                # Split comma-separated genres
                genres = [g.strip() for g in row[0].split(',')]
                all_genres.update(genres)
        
        conn.close()
        return sorted(list(all_genres))
    
    def get_statistics(self) -> dict:
        """Get database statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute("SELECT COUNT(*) FROM shows")
        stats['total_shows'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM shows WHERE type = 'movie'")
        stats['movies'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM shows WHERE type = 'series'")
        stats['series'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM shows WHERE type = 'anime'")
        stats['anime'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM episodes")
        stats['episodes'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM servers")
        stats['servers'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(imdb_rating) FROM shows WHERE imdb_rating IS NOT NULL")
        avg_rating = cursor.fetchone()[0]
        stats['avg_rating'] = round(avg_rating, 2) if avg_rating else 0
        
        conn.close()
        return stats

# Display functions
def display_shows_table(shows: List[dict], title: str = "Shows"):
    """Display shows in a table"""
    if not shows:
        console.print(f"[yellow]No shows found[/yellow]")
        return
    
    table = Table(title=title, show_header=True, header_style="bold blue")
    table.add_column("ID", style="dim", width=6)
    table.add_column("Title", style="cyan")
    table.add_column("Type", width=8)
    table.add_column("Year", width=6)
    table.add_column("Rating", width=8)
    
    for show in shows:
        table.add_row(
            str(show.get('id', '')),
            show.get('title', 'N/A')[:50],
            show.get('type', 'N/A'),
            str(show.get('year', 'N/A')),
            str(show.get('imdb_rating', 'N/A'))
        )
    
    console.print(table)
    console.print(f"\n[cyan]Total: {len(shows)} shows[/cyan]")

def display_show_details(show: dict):
    """Display detailed show information"""
    if not show:
        console.print("[red]Show not found[/red]")
        return
    
    console.print(Panel(f"[bold cyan]{show['title']}[/bold cyan]", expand=False))
    
    console.print(f"[bold]Type:[/bold] {show['type']}")
    console.print(f"[bold]Year:[/bold] {show.get('year', 'N/A')}")
    console.print(f"[bold]IMDb Rating:[/bold] {show.get('imdb_rating', 'N/A')}")
    console.print(f"[bold]Slug:[/bold] {show.get('slug', 'N/A')}")
    console.print(f"[bold]Source:[/bold] {show.get('source_url', 'N/A')}")
    
    if show.get('genres'):
        console.print(f"[bold]Genres:[/bold] {show['genres']}")
    
    if show.get('directors'):
        console.print(f"[bold]Directors:[/bold] {show['directors']}")
    
    if show.get('cast'):
        console.print(f"[bold]Cast:[/bold] {show['cast']}")
    
    if show.get('synopsis'):
        console.print(f"\n[bold]Synopsis:[/bold]\n{show['synopsis'][:300]}...")
    
    if show.get('seasons'):
        console.print(f"\n[bold]Seasons:[/bold]")
        for season in show['seasons']:
            poster_info = f" (poster: {season.get('poster', 'N/A')[:30]}...)" if season.get('poster') else ""
            console.print(f"  Season {season['season_number']}: {season['episode_count']} episodes{poster_info}")

def display_statistics(stats: dict):
    """Display database statistics"""
    console.print(Panel("[bold cyan]Database Statistics[/bold cyan]", expand=False))
    
    console.print(f"[bold]Total Shows:[/bold] {stats['total_shows']}")
    console.print(f"  • Movies: {stats['movies']}")
    console.print(f"  • Series: {stats['series']}")
    console.print(f"  • Anime: {stats['anime']}")
    console.print(f"[bold]Episodes:[/bold] {stats['episodes']}")
    console.print(f"[bold]Servers:[/bold] {stats['servers']}")
    console.print(f"[bold]Average Rating:[/bold] {stats['avg_rating']}/10")

def main():
    """Main CLI interface"""
    db = DatabaseQuery()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "search" and len(sys.argv) > 2:
            query = " ".join(sys.argv[2:])
            results = db.search_shows(query)
            display_shows_table(results, f"Search Results for '{query}'")
        
        elif command == "genre" and len(sys.argv) > 2:
            genre = sys.argv[2]
            results = db.get_shows_by_genre(genre)
            display_shows_table(results, f"Shows in Genre: {genre}")
        
        elif command == "trending":
            results = db.get_trending_shows(20)
            display_shows_table(results, "Trending Shows")
        
        elif command == "top":
            show_type = sys.argv[2] if len(sys.argv) > 2 else None
            results = db.get_top_rated(show_type, 20)
            display_shows_table(results, f"Top Rated {show_type or 'Shows'}")
        
        elif command == "recent":
            results = db.get_recent_shows(20)
            display_shows_table(results, "Recently Added")
        
        elif command == "show" and len(sys.argv) > 2:
            show_id = int(sys.argv[2])
            show = db.get_show_details(show_id)
            display_show_details(show)
        
        elif command == "genres":
            genres = db.get_all_genres()
            console.print(Panel("[bold cyan]All Genres[/bold cyan]", expand=False))
            console.print(", ".join(genres))
        
        elif command == "stats":
            stats = db.get_statistics()
            display_statistics(stats)
        
        else:
            console.print("[red]Unknown command[/red]")
            print_usage()
    else:
        # Default: show statistics and recent shows
        stats = db.get_statistics()
        display_statistics(stats)
        console.print()
        recent = db.get_recent_shows(10)
        display_shows_table(recent, "Recently Added Shows")

def print_usage():
    """Print usage instructions"""
    console.print("\n[bold cyan]Usage:[/bold cyan]")
    console.print("  py 03_query_database.py                    - Show stats and recent shows")
    console.print("  py 03_query_database.py search <query>     - Search shows")
    console.print("  py 03_query_database.py genre <slug>       - Shows by genre")
    console.print("  py 03_query_database.py trending           - Trending shows")
    console.print("  py 03_query_database.py top [type]         - Top rated shows")
    console.print("  py 03_query_database.py recent             - Recently added")
    console.print("  py 03_query_database.py show <id>          - Show details")
    console.print("  py 03_query_database.py genres             - List all genres")
    console.print("  py 03_query_database.py stats              - Database statistics")

if __name__ == "__main__":
    main()
