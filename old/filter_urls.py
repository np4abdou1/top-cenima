#!/usr/bin/env python3
"""
Filter series_animes.json to keep only main series URLs
Removes season-specific URLs (those containing 'الموسم')
"""

import json
import re
from pathlib import Path

def filter_urls(input_file: str, output_file: str = None, backup: bool = True):
    """
    Filter URLs to keep only main series/anime pages
    
    Args:
        input_file: Path to input JSON file
        output_file: Path to output file (defaults to input_file)
        backup: Create backup of original file
    """
    
    # Read the JSON file
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Check if it's a list or dict
    if isinstance(data, list):
        urls = data
    elif isinstance(data, dict) and 'urls' in data:
        urls = data['urls']
    else:
        print("❌ Unexpected JSON structure")
        return
    
    print(f"📊 Original URLs: {len(urls)}")
    
    # Filter URLs - keep only those WITHOUT 'الموسم' (season)
    filtered_urls = []
    removed_urls = []
    
    for url in urls:
        # Check if URL contains season indicator
        if 'الموسم' in url or '-الموسم-' in url:
            removed_urls.append(url)
        else:
            filtered_urls.append(url)
    
    print(f"✅ Kept URLs: {len(filtered_urls)}")
    print(f"❌ Removed URLs: {len(removed_urls)}")
    print(f"📉 Reduction: {len(removed_urls) / len(urls) * 100:.1f}%")
    
    # Show some examples
    if removed_urls:
        print("\n🗑️  Sample removed URLs:")
        for url in removed_urls[:5]:
            print(f"   - {url}")
        if len(removed_urls) > 5:
            print(f"   ... and {len(removed_urls) - 5} more")
    
    # Create backup if requested
    if backup and output_file != input_file:
        backup_file = input_file + '.backup'
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(urls if isinstance(data, list) else data, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Backup saved to: {backup_file}")
    
    # Save filtered URLs
    output_path = output_file or input_file
    
    if isinstance(data, list):
        output_data = filtered_urls
    else:
        output_data = data.copy()
        output_data['urls'] = filtered_urls
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Filtered URLs saved to: {output_path}")
    
    # Save removed URLs for reference
    removed_file = Path(output_path).parent / 'removed_season_urls.json'
    with open(removed_file, 'w', encoding='utf-8') as f:
        json.dump(removed_urls, f, ensure_ascii=False, indent=2)
    
    print(f"📝 Removed URLs saved to: {removed_file}")
    
    return filtered_urls, removed_urls

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Filter series URLs to remove season-specific pages')
    parser.add_argument('input', help='Input JSON file')
    parser.add_argument('-o', '--output', help='Output JSON file (default: overwrite input)')
    parser.add_argument('--no-backup', action='store_true', help='Do not create backup')
    
    args = parser.parse_args()
    
    filter_urls(
        args.input,
        args.output,
        backup=not args.no_backup
    )

if __name__ == '__main__':
    # If run without arguments, use default file
    import sys
    if len(sys.argv) == 1:
        input_file = r'c:\Users\abdou\Desktop\top cenima\data\series_animes.json'
        print(f"🔧 Using default file: {input_file}\n")
        filter_urls(input_file)
    else:
        main()
