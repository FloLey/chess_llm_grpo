import os
import io
import csv
import random
from collections import defaultdict
import zstandard as zstd
from tqdm import tqdm
import gc

# Configuration
compressed_filename = "lichess_db_puzzle.csv.zst"
csv_filename = "lichess_db_puzzle.csv"
puzzles_dir = "data/puzzles"

# Set a fixed random seed for reproducibility
random.seed(42)

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_dataset():
    """Download the compressed dataset if needed"""
    if not os.path.exists(compressed_filename):
        print(f"Downloading {compressed_filename}...")
        os.system(f"wget https://database.lichess.org/lichess_db_puzzle.csv.zst -O {compressed_filename}")
    else:
        print(f"{compressed_filename} already exists. Skipping download.")

def extract_dataset():
    """Extract the CSV file if needed"""
    if not os.path.exists(csv_filename):
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
    """Process puzzles and create rating-based files"""
    print("Loading CSV into memory...")
    rating_groups = defaultdict(list)
    
    # First pass: Read and group puzzles by rating
    with open(csv_filename, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for puzzle in tqdm(reader, desc="Processing puzzles"):
            rating_str = puzzle.get("Rating", "").strip()
            if rating_str.isdigit():
                rating = int(rating_str)
                # Group into 100-point rating bins
                rating_bin = (rating // 100) * 100
                rating_groups[rating_bin].append(puzzle)

    # Create output directory if it doesn't exist
    ensure_dir(puzzles_dir)

    # Write separate files for each rating group
    for rating_bin, puzzles in rating_groups.items():
        output_file = os.path.join(puzzles_dir, f"puzzles_{rating_bin}.csv")
        
        # Sort puzzles by theme
        puzzles.sort(key=lambda x: x.get("Themes", ""))
        
        print(f"Writing {len(puzzles)} puzzles to {output_file}")
        with open(output_file, "w", newline="", encoding="utf-8") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(puzzles)

def main():
    """Main execution function"""
    download_dataset()
    extract_dataset()
    process_puzzles()
    
    # Cleanup
    gc.collect()
    print("Processing complete. Files created in the puzzles directory.")

if __name__ == "__main__":
    main()
