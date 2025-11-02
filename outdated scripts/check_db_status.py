#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('data/scrapped.db')
cursor = conn.cursor()

print("=" * 60)
print("DATABASE STATUS")
print("=" * 60)

cursor.execute('SELECT status, COUNT(*) FROM scrape_progress GROUP BY status')
print('\nScrape Progress Status:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

cursor.execute('SELECT type, COUNT(*) FROM shows GROUP BY type')
print('\nShows by Type:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

cursor.execute('SELECT COUNT(*) FROM seasons')
print(f'\nTotal Seasons: {cursor.fetchone()[0]}')

cursor.execute('SELECT COUNT(*) FROM episodes')
print(f'Total Episodes: {cursor.fetchone()[0]}')

cursor.execute('SELECT COUNT(*) FROM servers')
print(f'Total Servers: {cursor.fetchone()[0]}')

conn.close()
