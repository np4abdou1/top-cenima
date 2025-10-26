#!/usr/bin/env python3
"""
Batch scraper for processing multiple URLs
Supports parallel processing and progress tracking
"""

import json
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional
import argparse

# Import the main scraping function
from scrape_single_test import run_single, cleanup

def scrape_url_safe(url: str) -> tuple[str, Optional[Dict], float]:
    """Scrape a single URL with error handling"""
    start_time = time.time()
    try:
        result = run_single(url)
        elapsed = time.time() - start_time
        return (url, result, elapsed)
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Error scraping {url}: {str(e)[:100]}")
        return (url, None, elapsed)
    finally:
        cleanup()

def load_urls(file_path: str) -> List[str]:
    """Load URLs from file (one per line)"""
    with open(file_path, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return urls

def save_result(result: Dict, output_dir: str, index: int):
    """Save individual result to JSON file"""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename from title or index
    title = result.get('title', f'item_{index}')
    # Clean filename
    filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
    filename = filename.replace(' ', '_')[:50]  # Limit length
    
    filepath = os.path.join(output_dir, f"{index:04d}_{filename}.json")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    return filepath

def scrape_batch(urls: List[str], output_dir: str = 'data/batch', workers: int = 1, resume: bool = False):
    """
    Scrape multiple URLs with parallel processing
    
    Args:
        urls: List of URLs to scrape
        output_dir: Directory to save results
        workers: Number of parallel workers (1 = sequential)
        resume: Skip already scraped URLs
    """
    
    print(f"🚀 Starting batch scrape")
    print(f"📊 Total URLs: {len(urls)}")
    print(f"👷 Workers: {workers}")
    print(f"📁 Output: {output_dir}")
    print("=" * 60)
    
    # Track progress
    completed = 0
    failed = 0
    total_time = 0
    start_time = time.time()
    
    # Check for existing files if resume mode
    existing_files = set()
    if resume and os.path.exists(output_dir):
        existing_files = {f for f in os.listdir(output_dir) if f.endswith('.json')}
        print(f"📂 Found {len(existing_files)} existing files (resume mode)")
    
    # Progress file
    progress_file = os.path.join(output_dir, '_progress.json')
    
    if workers == 1:
        # Sequential processing
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Processing: {url}")
            
            url_result, result, elapsed = scrape_url_safe(url)
            total_time += elapsed
            
            if result:
                filepath = save_result(result, output_dir, i)
                completed += 1
                print(f"✅ Saved to: {filepath} ({elapsed:.1f}s)")
            else:
                failed += 1
                print(f"❌ Failed ({elapsed:.1f}s)")
            
            # Show stats
            avg_time = total_time / i
            remaining = (len(urls) - i) * avg_time
            print(f"📈 Progress: {i}/{len(urls)} | Avg: {avg_time:.1f}s | ETA: {remaining/60:.1f}m")
    
    else:
        # Parallel processing
        with ProcessPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_url = {executor.submit(scrape_url_safe, url): (i, url) 
                           for i, url in enumerate(urls, 1)}
            
            # Process completed tasks
            for future in as_completed(future_to_url):
                i, url = future_to_url[future]
                
                try:
                    url_result, result, elapsed = future.result()
                    total_time += elapsed
                    
                    if result:
                        filepath = save_result(result, output_dir, i)
                        completed += 1
                        print(f"✅ [{completed + failed}/{len(urls)}] {result.get('title', 'Unknown')} ({elapsed:.1f}s)")
                    else:
                        failed += 1
                        print(f"❌ [{completed + failed}/{len(urls)}] Failed: {url[:50]}... ({elapsed:.1f}s)")
                    
                    # Show progress
                    if (completed + failed) % 10 == 0:
                        elapsed_total = time.time() - start_time
                        avg_time = elapsed_total / (completed + failed)
                        remaining = (len(urls) - completed - failed) * avg_time
                        print(f"📈 Progress: {completed + failed}/{len(urls)} | Success: {completed} | Failed: {failed} | ETA: {remaining/60:.1f}m")
                
                except Exception as e:
                    failed += 1
                    print(f"❌ Exception: {str(e)[:100]}")
    
    # Final summary
    total_elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("🏁 Batch scraping completed!")
    print(f"✅ Successful: {completed}")
    print(f"❌ Failed: {failed}")
    print(f"⏱️  Total time: {total_elapsed/60:.1f} minutes ({total_elapsed/3600:.2f} hours)")
    print(f"📊 Average time: {total_time/len(urls):.1f}s per item")
    print(f"📁 Results saved to: {output_dir}")
    
    # Save summary
    summary = {
        "total": len(urls),
        "completed": completed,
        "failed": failed,
        "total_time_seconds": total_elapsed,
        "average_time_seconds": total_time / len(urls) if urls else 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(os.path.join(output_dir, '_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

def main():
    parser = argparse.ArgumentParser(description='Batch scrape TopCinema URLs')
    parser.add_argument('input', help='Input file with URLs (one per line)')
    parser.add_argument('-o', '--output', default='data/batch', help='Output directory')
    parser.add_argument('-w', '--workers', type=int, default=1, help='Number of parallel workers')
    parser.add_argument('-r', '--resume', action='store_true', help='Resume from existing files')
    
    args = parser.parse_args()
    
    # Load URLs
    urls = load_urls(args.input)
    
    if not urls:
        print("❌ No URLs found in input file")
        sys.exit(1)
    
    # Run batch scrape
    scrape_batch(urls, args.output, args.workers, args.resume)

if __name__ == '__main__':
    main()
