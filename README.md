# TopCinema Advanced Batch Scraper

High-performance, production-ready scraper with SQLite database, real-time storage, and resume capability.

## Features

✅ **Real-time Database Storage** - Data saved immediately to SQLite  
✅ **Resume Capability** - Pause and continue anytime (Ctrl+C safe)  
✅ **Multi-threaded** - 10 parallel workers for maximum speed  
✅ **Interrupt Protection** - Graceful shutdown on Ctrl+C  
✅ **Cross-platform** - Works on Windows & Linux  
✅ **Progress Tracking** - Real-time stats and ETA  
✅ **Error Handling** - Failed items can be retried  
✅ **Clean Database Schema** - Normalized tables with indexes  

## Installation

```bash
# Install dependencies
pip install requests beautifulsoup4 rich

# Or use requirements (if created)
pip install -r requirements.txt
```

## Quick Start

### 1. Scrape All Movies & Series

```bash
python batch_scraper.py
```

This will:
- Load URLs from `data/movies.json` and `data/series_animes.json`
- Scrape all content with 10 parallel workers
- Save to `data/topcinema.db` in real-time
- Show progress and ETA

### 2. View Statistics

```bash
python db_viewer.py --stats
```

Output:
```
📊 Database Statistics
==================================================
Total Media: 6353
  Movies: 2963
  Series: 3390

Status:
  Completed: 1250
  Pending: 5000
  Errors: 103

Content:
  Seasons: 8500
  Episodes: 125000
  Servers: 950000
==================================================
```

### 3. Search Database

```bash
python db_viewer.py --search "Interstellar"
```

### 4. Export to JSON

```bash
python db_viewer.py --export output.json
```

### 5. View Errors

```bash
python db_viewer.py --errors
```

### 6. Retry Failed Items

```bash
# Reset errors to pending
python db_viewer.py --reset-errors

# Run scraper again
python batch_scraper.py
```

## Performance

### Time Estimates

**Current Setup (10 workers)**:
- **2,963 movies**: ~5.8 hours
- **3,390 series**: ~48 hours
- **Total**: ~53.8 hours (~2.2 days)

**With 20 workers**:
- **Total**: ~27 hours (~1.1 days)

**With 50 workers** (if server allows):
- **Total**: ~11 hours

### Speed Optimization Tips

1. **Increase Workers**:
   ```python
   # In batch_scraper.py
   MAX_WORKERS = 20  # Increase from 10
   ```

2. **Run Multiple Instances**:
   ```bash
   # Terminal 1 - Movies only
   python batch_scraper_movies.py
   
   # Terminal 2 - Series only
   python batch_scraper_series.py
   ```

3. **Use SSD** - Store database on SSD for faster I/O

4. **Increase Network Timeout** - If you have slow connection

## Database Schema

```
media (main table)
├── id, url, title, type, year, imdb_rating
├── poster, synopsis, trailer
└── status, error, scraped_at

metadata (key-value pairs)
├── media_id → media.id
└── key, value (category, genres, cast, etc.)

seasons
├── media_id → media.id
└── season_number, poster

episodes
├── season_id → seasons.id
└── episode_number

servers
├── episode_id → episodes.id
└── server_number, embed_url
```

## Resume & Interrupt Protection

The scraper is **100% safe to interrupt**:

1. Press **Ctrl+C** anytime
2. Current tasks finish gracefully
3. All data is saved
4. Run again to continue from where you stopped

```bash
# Start scraping
python batch_scraper.py

# Press Ctrl+C to pause
^C
⚠️  Interrupt received! Finishing current tasks...

# Run again to resume
python batch_scraper.py
# Automatically continues from where it stopped
```

## Advanced Usage

### Query Database Directly

```python
import sqlite3

conn = sqlite3.connect('data/topcinema.db')
cursor = conn.cursor()

# Get all movies with IMDB > 8
cursor.execute('''
    SELECT title, imdb_rating 
    FROM media 
    WHERE type = 'movie' AND imdb_rating > 8
    ORDER BY imdb_rating DESC
''')

for row in cursor.fetchall():
    print(row)
```

### Export Specific Data

```python
from db_viewer import DatabaseViewer

viewer = DatabaseViewer()

# Get specific media
media = viewer.get_media_full(media_id=123)
print(media)

# Search
results = viewer.search_media("Breaking Bad")
```

## Logs

All activity is logged to:
- **Console**: Real-time progress
- **File**: `data/scraper.log`

## Troubleshooting

### "Database is locked"
- Only one scraper instance can run at a time
- Wait for current instance to finish or kill it

### "Too many requests" / 403 errors
- Reduce `MAX_WORKERS` in `batch_scraper.py`
- Add delay between requests

### Memory issues
- Reduce `MAX_WORKERS`
- Close other applications

### Slow performance
- Check internet speed
- Increase `MAX_WORKERS` if CPU/network allows
- Use SSD for database

## File Structure

```
top cenima/
├── batch_scraper.py          # Main batch scraper
├── db_viewer.py               # Database viewer/query tool
├── scrape_single_test.py      # Single URL scraper (imported)
├── scrape_movie_urls.py       # URL collector
├── data/
│   ├── movies.json            # Movie URLs
│   ├── series_animes.json     # Series/Anime URLs
│   ├── topcinema.db          # SQLite database
│   └── scraper.log           # Scraping logs
└── README.md                  # This file
```

## Production Deployment

### Linux Server

```bash
# Run in background with nohup
nohup python batch_scraper.py > scraper.out 2>&1 &

# Or use screen
screen -S scraper
python batch_scraper.py
# Ctrl+A, D to detach

# Reattach later
screen -r scraper
```

### Windows Server

```powershell
# Run in background
Start-Process python -ArgumentList "batch_scraper.py" -WindowStyle Hidden
```

### Docker (Optional)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install requests beautifulsoup4 rich
CMD ["python", "batch_scraper.py"]
```

## License

MIT License - Free to use and modify

## Support

For issues or questions, check the logs in `data/scraper.log`
