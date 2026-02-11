import requests
import csv
import time
import sys
import os
import string
import urllib.parse

# ==============================================================================
# CONFIGURATION
# ==============================================================================
JW_API_SECRET = os.getenv("JW_API_SECRET")
if not JW_API_SECRET:
    raise ValueError("❌ Error: JW_API_SECRET environment variable is not set.")
JW_SITE_ID = "LKnEu5Hs"
GCS_INVENTORY_FILE = "adlandvideosfile_gcs.txt" 
OUTPUT_CSV = "TwelveLabs_Master_Manifest.csv" # Temporary "Master" file
BASE_URL = f"https://api.jwplayer.com/v2/sites/{JW_SITE_ID}/media"

def load_gcs_inventory(filepath):
    print(f"--> Loading GCS inventory from: {filepath}...")
    gcs_files = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                # Extract filename: gs://bucket/folder/video.mp4 -> video.mp4
                filename = line.strip().split('/')[-1]
                if filename: gcs_files.add(filename)
        print(f"✅ Successfully loaded {len(gcs_files)} files from GCS.")
        return gcs_files
    except FileNotFoundError:
        print(f"❌ Error: GCS inventory file '{filepath}' not found.")
        sys.exit(1)

def fetch_bucket(prefix, writer, gcs_files):
    """
    Fetches all videos where the ID starts with the given prefix.
    Range: {prefix}0000000 to {prefix}zzzzzzz
    """
    start_id = f"{prefix}0000000"
    end_id = f"{prefix}zzzzzzz"
    query = f"q=id:[{start_id} TO {end_id}]"
    
    page = 1
    total_in_bucket = 0
    print(f"🔄 Processing ID Prefix '{prefix}'... ", end="", flush=True)
    
    while True:
        url = f"{BASE_URL}?page_length=100&{query}&page={page}"
        try:
            resp = requests.get(url, headers={"Authorization": f"Bearer {JW_API_SECRET}"})
            
            # Rate Limit Handling
            if resp.status_code == 429:
                time.sleep(2)
                continue
            
            if resp.status_code != 200:
                print(f"Error {resp.status_code}")
                break
                
            data = resp.json()
            media = data.get('media', [])
            
            if not media: break
            
            for video in media:
                # --- BASIC DATA ---
                jw_id = video.get('id')
                date = video.get('created')
                source_url = video.get('source_url', '')
                
                # --- METADATA ---
                meta = video.get('metadata', {})
                title = meta.get('title', '')
                
                # --- SNIPPET (Description truncated to 200 chars) ---
                desc = meta.get('description', '')
                if desc is None: desc = ""
                # Clean newlines to avoid breaking CSV format
                snippet = desc[:200].replace('\n', ' ').replace('\r', '') 
                
                # --- TAGS (Comma separated string) ---
                tags_list = meta.get('tags', [])
                tags_str = ",".join(tags_list)
                
                # --- GCS MATCHING ---
                extracted = source_url.split('/')[-1] if source_url else ""
                final_name = extracted if extracted in gcs_files else ""
                
                # --- ARTICLE URL (Custom params) ---
                params = meta.get('custom_params', {})
                url_art = params.get('article_url', params.get('url', ''))

                # Write row to Master CSV
                writer.writerow([jw_id, final_name, url_art, title, date, jw_id, snippet, tags_str])
            
            count = len(media)
            total_in_bucket += count
            
            if count < 100: break
            page += 1
            
        except Exception as e:
            print(f"Exception: {e}")
            break
            
    print(f"✅ {total_in_bucket}")
    return total_in_bucket

def main():
    print("===================================================")
    print("   MASTER HARVESTER (WITH SNIPPETS & TAGS)         ")
    print("===================================================")
    
    gcs_filenames = load_gcs_inventory(GCS_INVENTORY_FILE)
    
    print(f"--> Initializing output file: {OUTPUT_CSV}")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Master File Headers
        writer.writerow(['Unique_ID', 'Filename', 'URL', 'Title', 'Date', 'JW_ID', 'Snippet', 'Tags_Raw'])
        
    # Iterate through digits, lowercase, and uppercase letters
    prefixes = string.digits + string.ascii_lowercase + string.ascii_uppercase
    
    grand_total = 0
    for char in prefixes:
        grand_total += fetch_bucket(char, csv.writer(open(OUTPUT_CSV, 'a', encoding='utf-8')), gcs_filenames)
        
    print(f"\n🎉 MASTER HARVEST COMPLETE. Total Videos: {grand_total}")
    print(f"📁 File saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()