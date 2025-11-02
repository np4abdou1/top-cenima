"""
Database verification script - checks structure and sample data
"""
import sqlite3
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

def check_database():
    conn = sqlite3.connect("data/scraper.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    console.print(Panel("[bold cyan]Database Structure Verification[/bold cyan]", expand=False))
    
    # 1. Check all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    console.print(f"\n[bold]Tables found:[/bold] {', '.join(tables)}")
    
    # 2. Check shows table structure
    console.print("\n[bold yellow]═══ SHOWS TABLE ═══[/bold yellow]")
    cursor.execute("PRAGMA table_info(shows)")
    shows_cols = cursor.fetchall()
    
    table = Table(title="Shows Columns", show_header=True)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Not Null", style="yellow")
    
    for col in shows_cols:
        table.add_row(col['name'], col['type'], "Yes" if col['notnull'] else "No")
    console.print(table)
    
    # Sample show data
    cursor.execute("SELECT id, title, year, type, imdb_rating, genres FROM shows LIMIT 3")
    console.print("\n[bold]Sample Shows:[/bold]")
    for row in cursor.fetchall():
        console.print(f"  • ID {row['id']}: {row['title']} ({row['year']}) - {row['type']} - Rating: {row['imdb_rating']}")
        console.print(f"    Genres: {row['genres']}")
    
    # 3. Check seasons table
    console.print("\n[bold yellow]═══ SEASONS TABLE ═══[/bold yellow]")
    cursor.execute("PRAGMA table_info(seasons)")
    seasons_cols = cursor.fetchall()
    
    table = Table(title="Seasons Columns", show_header=True)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    
    for col in seasons_cols:
        table.add_row(col['name'], col['type'])
    console.print(table)
    
    # Sample season data
    cursor.execute("""
        SELECT s.id, s.show_id, s.season_number, s.poster, sh.title
        FROM seasons s
        JOIN shows sh ON s.show_id = sh.id
        LIMIT 5
    """)
    console.print("\n[bold]Sample Seasons:[/bold]")
    for row in cursor.fetchall():
        poster_preview = row['poster'][:50] + "..." if row['poster'] else "None"
        console.print(f"  • Season {row['season_number']} of '{row['title']}' - Poster: {poster_preview}")
    
    # 4. Check episodes table
    console.print("\n[bold yellow]═══ EPISODES TABLE ═══[/bold yellow]")
    cursor.execute("PRAGMA table_info(episodes)")
    episodes_cols = cursor.fetchall()
    
    table = Table(title="Episodes Columns", show_header=True)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    
    for col in episodes_cols:
        table.add_row(col['name'], col['type'])
    console.print(table)
    
    # Episode count per season
    cursor.execute("""
        SELECT s.show_id, sh.title, s.season_number, COUNT(e.id) as ep_count
        FROM seasons s
        JOIN shows sh ON s.show_id = sh.id
        LEFT JOIN episodes e ON s.id = e.season_id
        GROUP BY s.id
        LIMIT 5
    """)
    console.print("\n[bold]Sample Episode Counts:[/bold]")
    for row in cursor.fetchall():
        console.print(f"  • '{row['title']}' Season {row['season_number']}: {row['ep_count']} episodes")
    
    # 5. Check servers table
    console.print("\n[bold yellow]═══ SERVERS TABLE ═══[/bold yellow]")
    cursor.execute("PRAGMA table_info(servers)")
    servers_cols = cursor.fetchall()
    
    table = Table(title="Servers Columns", show_header=True)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    
    for col in servers_cols:
        table.add_row(col['name'], col['type'])
    console.print(table)
    
    # Server count per episode
    cursor.execute("""
        SELECT e.id, COUNT(s.id) as server_count
        FROM episodes e
        LEFT JOIN servers s ON e.id = s.episode_id
        GROUP BY e.id
        LIMIT 5
    """)
    console.print("\n[bold]Sample Server Counts per Episode:[/bold]")
    for row in cursor.fetchall():
        console.print(f"  • Episode ID {row['id']}: {row['server_count']} servers")
    
    # 6. Check progress table
    console.print("\n[bold yellow]═══ SCRAPE PROGRESS TABLE ═══[/bold yellow]")
    cursor.execute("PRAGMA table_info(scrape_progress)")
    progress_cols = cursor.fetchall()
    
    table = Table(title="Progress Columns", show_header=True)
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    
    for col in progress_cols:
        table.add_row(col['name'], col['type'])
    console.print(table)
    
    # Progress stats
    cursor.execute("SELECT status, COUNT(*) as count FROM scrape_progress GROUP BY status")
    console.print("\n[bold]Scraping Progress:[/bold]")
    for row in cursor.fetchall():
        console.print(f"  • {row['status']}: {row['count']} URLs")
    
    # 7. Data integrity checks
    console.print("\n[bold yellow]═══ DATA INTEGRITY CHECKS ═══[/bold yellow]")
    
    # Check for shows without seasons
    cursor.execute("SELECT COUNT(*) FROM shows WHERE id NOT IN (SELECT DISTINCT show_id FROM seasons)")
    orphan_shows = cursor.fetchone()[0]
    status = "✓" if orphan_shows == 0 else "✗"
    console.print(f"{status} Shows without seasons: {orphan_shows}")
    
    # Check for seasons without episodes
    cursor.execute("SELECT COUNT(*) FROM seasons WHERE id NOT IN (SELECT DISTINCT season_id FROM episodes)")
    orphan_seasons = cursor.fetchone()[0]
    status = "✓" if orphan_seasons == 0 else "✗"
    console.print(f"{status} Seasons without episodes: {orphan_seasons}")
    
    # Check for episodes without servers
    cursor.execute("SELECT COUNT(*) FROM episodes WHERE id NOT IN (SELECT DISTINCT episode_id FROM servers)")
    orphan_episodes = cursor.fetchone()[0]
    status = "✓" if orphan_episodes == 0 else "✗"
    console.print(f"{status} Episodes without servers: {orphan_episodes}")
    
    # Check for NULL years
    cursor.execute("SELECT COUNT(*) FROM shows WHERE year IS NULL")
    null_years = cursor.fetchone()[0]
    console.print(f"Shows with NULL year: {null_years}")
    
    # Check for NULL posters in seasons
    cursor.execute("SELECT COUNT(*) FROM seasons WHERE poster IS NULL")
    null_posters = cursor.fetchone()[0]
    console.print(f"Seasons with NULL poster: {null_posters}")
    
    # 8. Overall summary
    console.print("\n[bold green]═══ OVERALL SUMMARY ═══[/bold green]")
    cursor.execute("SELECT COUNT(*) FROM shows")
    total_shows = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM seasons")
    total_seasons = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM episodes")
    total_episodes = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM servers")
    total_servers = cursor.fetchone()[0]
    
    console.print(f"✓ Total Shows: [cyan]{total_shows}[/cyan]")
    console.print(f"✓ Total Seasons: [cyan]{total_seasons}[/cyan]")
    console.print(f"✓ Total Episodes: [cyan]{total_episodes}[/cyan]")
    console.print(f"✓ Total Servers: [cyan]{total_servers}[/cyan]")
    console.print(f"✓ Avg Episodes per Show: [cyan]{total_episodes/total_shows:.1f}[/cyan]")
    console.print(f"✓ Avg Servers per Episode: [cyan]{total_servers/total_episodes:.1f}[/cyan]")
    
    conn.close()

if __name__ == "__main__":
    check_database()
