# Chess Puzzles Dataset

A comprehensive collection of chess puzzles from Lichess.org, organized by rating and puzzle type.

## Structure

The dataset is organized in two main ways:

### By Rating Range
Located in `data/puzzles/datasets/`, puzzles are grouped into rating ranges (e.g., 1000-1200, 1200-1400, etc.) with separate test and train sets.

### By Puzzle Type and Rating
Located in `data/puzzles/sorted/`, puzzles are first organized by rating (e.g., 1000, 1100, etc.) and then by type (e.g., fork, pin, mate in 2, etc.).

## Puzzle Types Include:
- Tactical patterns (fork, pin, skewer, etc.)
- Checkmate patterns (back rank mate, smothered mate, etc.)
- Endgame positions
- And many more

## Scripts
Python scripts for dataset creation and processing are located in `data/scripts/`.

## Requirements
Required Python packages:
- tqdm 4.65.0
- zstandard 0.21.0  
- requests 2.31.0

Install dependencies with:
```pip install -r requirements.txt```

## Usage
The datasets are provided in CSV format and can be used for:
- Training chess engines
- Creating chess puzzle applications
- Analyzing chess patterns
- Educational purposes
