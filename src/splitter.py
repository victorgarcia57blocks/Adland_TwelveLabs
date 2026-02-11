import pandas as pd
import sys
import os

# Input File
INPUT_FILE = "TwelveLabs_Master_Manifest.csv"

def main():
    print("===================================================")
    print("   METADATA SPLITTER & NORMALIZER                  ")
    print("===================================================")

    print(f"--> Reading Master File: {INPUT_FILE}...")
    
    if not os.path.exists(INPUT_FILE):
        print("❌ Error: Master file not found. Please run 'master_harvester.py' first.")
        sys.exit(1)

    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)
        
    print(f"--> Processing {len(df)} video records...")

    # ==========================================
    # 1. GENERATE: Video_manifest.csv
    # ==========================================
    # Required Columns: Unique ID, Filename, URL, Title, Date, JW_id, Snippet
    print("--> Generating 'Video_manifest.csv'...")
    
    manifest_cols = ['Unique_ID', 'Filename', 'URL', 'Title', 'Date', 'JW_ID', 'Snippet']
    
    # Fill NaN values in Snippet with empty string to avoid errors
    if 'Snippet' in df.columns:
        df['Snippet'] = df['Snippet'].fillna('')
    else:
        df['Snippet'] = ''

    # Create a copy with only the required columns
    df_manifest = df[manifest_cols].copy()
    
    # Save to CSV
    df_manifest.to_csv("Video_manifest.csv", index=False)
    print("✅ Created: Video_manifest.csv")

    # ==========================================
    # 2. GENERATE: Tags.csv (Unique List)
    # ==========================================
    print("--> Extracting and normalizing tags...")
    
    unique_tags = set()
    
    # Iterate through the 'Tags_Raw' column to find all unique tags
    if 'Tags_Raw' in df.columns:
        for tags_str in df['Tags_Raw']:
            if pd.isna(tags_str) or tags_str == "":
                continue
            
            try:
                # Split by comma and strip whitespace
                tags = [t.strip() for t in str(tags_str).split(',')]
                for t in tags:
                    if t: unique_tags.add(t)
            except:
                continue
    
    # Create DataFrame for Tags with IDs
    tags_list = sorted(list(unique_tags))
    df_tags = pd.DataFrame(tags_list, columns=['Tag_name'])
    
    # Assign a unique Tag_ID (incremental integer)
    df_tags['Tag_ID'] = range(1, len(df_tags) + 1)
    
    # Reorder columns: Tag_ID, Tag_name
    df_tags = df_tags[['Tag_ID', 'Tag_name']]
    
    df_tags.to_csv("Tags.csv", index=False)
    print(f"✅ Created: Tags.csv ({len(df_tags)} unique tags found)")

    # ==========================================
    # 3. GENERATE: Video_tags.csv (Relational Table)
    # ==========================================
    print("--> Mapping Videos to Tags (Many-to-Many)...")
    
    # Create a lookup dictionary: Tag Name -> Tag ID
    tag_map = dict(zip(df_tags['Tag_name'], df_tags['Tag_ID']))
    
    video_tags_rows = []
    
    if 'Tags_Raw' in df.columns:
        for index, row in df.iterrows():
            vid_id = row['Unique_ID']
            tags_str = row['Tags_Raw']
            
            if pd.isna(tags_str) or tags_str == "":
                continue
                
            try:
                tags = [t.strip() for t in str(tags_str).split(',')]
                for t in tags:
                    # If the tag exists in our map, create the relationship
                    if t in tag_map:
                        video_tags_rows.append({
                            'Unique_ID': vid_id,
                            'Tag_ID': tag_map[t]
                        })
            except:
                continue
                
    df_video_tags = pd.DataFrame(video_tags_rows)
    df_video_tags.to_csv("Video_tags.csv", index=False)
    print(f"✅ Created: Video_tags.csv ({len(df_video_tags)} relationships mapped)")
    
    print("\n🎉 PROCESS COMPLETE! All 3 files are ready for delivery.")

if __name__ == "__main__":
    main()