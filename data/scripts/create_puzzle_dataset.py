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
datasets_dir = os.path.join("data", "puzzles", "datasets")
base_puzzles_dir = os.path.join("data", "puzzles")

# Set a fixed random seed for reproducibility
random.seed(42)

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
    if os.path.exists(puzzles_dir):
        print("Sorted puzzles directory already exists. Skipping processing.")
        return
        
    print("Loading CSV into memory...")
    
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

def create_datasets():
    """Create training and test datasets using sliding windows of rating ranges"""
    if not os.path.exists(puzzles_dir):
        print("Sorted puzzles directory doesn't exist. Run processing first.")
        return
        
    if os.path.exists(datasets_dir):
        print("Datasets directory already exists. Skipping dataset creation.")
        return
        
    ensure_dir(datasets_dir)
    
    # Get sorted list of rating directories
    rating_dirs = sorted([d for d in os.listdir(puzzles_dir) 
                         if os.path.isdir(os.path.join(puzzles_dir, d))],
                        key=int)
    
    if not rating_dirs:
        return
        
    def process_rating_range(rating_dir):
        """Process puzzles from a single rating directory"""
        train_puzzles = []
        test_puzzles = []
        rating_path = os.path.join(puzzles_dir, rating_dir)
        
        for theme_file in os.listdir(rating_path):
            if not theme_file.endswith('.csv'):
                continue
                
            theme_path = os.path.join(rating_path, theme_file)
            with open(theme_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                puzzles = list(reader)
                
                total_puzzles = len(puzzles)
                if total_puzzles == 0:
                    continue
                elif total_puzzles == 1:
                    test_size = 1
                    train_size = 0
                else:
                    test_size = min(10, total_puzzles // 3)
                    train_size = min(20, total_puzzles - test_size)
                
                random.shuffle(puzzles)
                test_puzzles.extend(puzzles[:test_size])
                if train_size > 0:
                    train_puzzles.extend(puzzles[test_size:test_size + train_size])
                    
        return train_puzzles, test_puzzles
    
    # Process sliding windows
    for i in tqdm(range(len(rating_dirs)), desc="Processing rating windows"):
        if i == 0:
            # First window: single rating range
            window_dirs = [rating_dirs[0]]
            window_name = f"range_{rating_dirs[0]}"
        elif i == 1:
            # Second window: first two rating ranges
            window_dirs = rating_dirs[:2]
            window_name = f"range_{rating_dirs[0]}_{rating_dirs[1]}"
        elif i >= len(rating_dirs) - 3:
            # Last window: last three rating ranges
            window_dirs = rating_dirs[-3:]
            window_name = f"range_{window_dirs[0]}_{window_dirs[-1]}"
            if i > len(rating_dirs) - 3:
                continue
        else:
            # Regular sliding window of three ranges
            window_dirs = rating_dirs[i-1:i+2]
            window_name = f"range_{window_dirs[0]}_{window_dirs[-1]}"
        
        # Create directory for this window
        window_dir = os.path.join(datasets_dir, window_name)
        ensure_dir(window_dir)
        
        # Process all ranges in this window
        all_train_puzzles = []
        all_test_puzzles = []
        for rating_dir in window_dirs:
            train, test = process_rating_range(rating_dir)
            all_train_puzzles.extend(train)
            all_test_puzzles.extend(test)
        
        # Write combined files for this window
        if all_test_puzzles:
            test_file = os.path.join(window_dir, 'test.csv')
            with open(test_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=all_test_puzzles[0].keys())
                writer.writeheader()
                writer.writerows(all_test_puzzles)
                
        if all_train_puzzles:
            train_file = os.path.join(window_dir, 'train.csv')
            with open(train_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=all_train_puzzles[0].keys())
                writer.writeheader()
                writer.writerows(all_train_puzzles)

def main():
    """Main execution function"""
    if not os.path.exists(csv_filename):
        if not os.path.exists(compressed_filename):
            download_dataset()
        if not os.path.exists(csv_filename):
            extract_dataset()
    
    process_puzzles()
    create_datasets()
    gc.collect()

if __name__ == "__main__":
    main()
