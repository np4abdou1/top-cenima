"""
TopCinema Scraper - Simple Runner
Minimal console output: WORKING° check IP:8080
"""
import os
import sys
import socket

def get_local_ip():
    """Get local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "localhost"

# Clear screen
os.system('cls' if os.name == 'nt' else 'clear')

# Get IP
ip = get_local_ip()

# Print single line
print(f"WORKING° check {ip}:8080")
sys.stdout.flush()

# Now run the actual scraper
exec(open('02_scraper_with_db.py').read())
