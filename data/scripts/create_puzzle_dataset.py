import os
import io
import csv
import random
from collections import defaultdict
import zstandard as zstd
from tqdm import tqdm
import gc
import requests

# Configuration
compressed_filename = os.path.join("data", "puzzles", "lichess_db_puzzle.csv.zst")
csv_filename = os.path.join("data", "puzzles", "lichess_db_puzzle.csv")
puzzles_dir = os.path.join("data", "puzzles", "sorted")
base_puzzles_dir = os.path.join("data", "puzzles")

# Set a fixed random seed for reproducibility
random.seed(42)

# Ensure base puzzles directory exists
ensure_dir(base_puzzles_dir)

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_dataset():
    """Download the compressed dataset if needed"""
    if not os.path.exists(compressed_filename):
        # Ensure the parent directory exists
        ensure_dir(os.path.dirname(compressed_filename))
        
        print(f"Downloading {compressed_filename}...")
        import requests
        url = "https://database.lichess.org/lichess_db_puzzle.csv.zst"
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        
        with open(compressed_filename, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
    else:
        print(f"{compressed_filename} already exists. Skipping download.")

def extract_dataset():
    """Extract the CSV file if needed"""
    if not os.path.exists(csv_filename):
        # Ensure the parent directory exists
        ensure_dir(os.path.dirname(csv_filename))
        
        print(f"Extracting {csv_filename} from {compressed_filename}...")
        with open(compressed_filename, "rb") as comp_file:
            with zstd.ZstdDecompressor().stream_reader(comp_file) as reader:
                text_stream = io.TextIOWrapper(reader, encoding="utf-8")
                with open(csv_filename, "w", newline="", encoding="utf-8") as out_file:
                    writer = csv.writer(out_file)
                    for line in tqdm(text_stream, desc="Extracting lines"):
                        writer.writerow(line.strip().split(","))
    else:
        print(f"{csv_filename} already exists. Skipping extraction.")

def process_puzzles():
    """Process puzzles and create rating and theme based files"""
    print("Loading CSV into memory...")
    
    # Clear existing puzzles directory
    if os.path.exists(puzzles_dir):
        import shutil
        shutil.rmtree(puzzles_dir)
    
    # Dictionary to store puzzles by rating bin and theme
    rating_theme_groups = defaultdict(lambda: defaultdict(list))
    
    with open(csv_filename, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for puzzle in tqdm(reader, desc="Processing puzzles"):
            rating_str = puzzle.get("Rating", "").strip()
            if not rating_str.isdigit():
                continue
                
            rating = int(rating_str)
            rating_bin = (rating // 100) * 100
            
            # Get themes and add puzzle to each theme's list
            themes_str = puzzle.get("Themes", "").strip()
            if themes_str:
                themes = themes_str.split()
                for theme in themes:
                    rating_theme_groups[rating_bin][theme].append(puzzle)

    # Create output directories and files
    for rating_bin in tqdm(sorted(rating_theme_groups.keys()), desc="Processing rating bins"):
        rating_dir = os.path.join(puzzles_dir, str(rating_bin))
        ensure_dir(rating_dir)
        
        for theme, puzzles in tqdm(rating_theme_groups[rating_bin].items(), 
                                 desc=f"Rating {rating_bin}", leave=False):
            output_file = os.path.join(rating_dir, f"{theme}.csv")
            with open(output_file, "w", newline="", encoding="utf-8") as out_file:
                writer = csv.DictWriter(out_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(puzzles)

def main():
    """Main execution function"""
    if not os.path.exists(csv_filename):
        if not os.path.exists(compressed_filename):
            download_dataset()
        if not os.path.exists(csv_filename):
            extract_dataset()
    
    process_puzzles()
    gc.collect()

if __name__ == "__main__":
    main()
