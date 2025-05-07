# Trie Storage

A Go application that processes gzipped CSV files and stores them in a trie-based directory structure.

## Features

- Reads gzipped CSV files from a source directory
- Processes the most recent file from each subdirectory
- Parallel processing using multiple CPU cores for improved performance
- Stores data in a trie-based directory structure
- Optional property history tracking with data lineage
- Supports different destination roots for asset and company files
- Real-time progress tracking with visual progress bar
- Colorful console output for better visibility
- Configurable metadata and analysis file locations
- Ability to resume processing from a specific file

## Requirements

- Go 1.21 or later

## Usage

### Basic Usage
```bash
go run . -source /path/to/source \
         -asset-dest /path/to/asset/destination \
         -company-dest /path/to/company/destination \
         -metadata /path/to/metadata.json \
         -analysis /path/to/analysis.json \
         -history
```

### Resuming Processing
To resume processing from a specific point (e.g., after processing 5 files):
```bash
go run . -source /path/to/source \
         -asset-dest /path/to/asset/destination \
         -company-dest /path/to/company/destination \
         -skip 5
```

### Command Line Arguments

- `-source`: Root path containing the source CSV files (required)
- `-asset-dest`: Destination root path for asset files (required)
- `-company-dest`: Destination root path for company files (required)
- `-metadata`: Path to metadata.json file (default: "metadata.json")
- `-analysis`: Path to analysis.json file (default: "analysis.json")
- `-history`: Enable history tracking (default: false)
- `-skip`: Number of files to skip (for resuming processing) (default: 0)

## Performance Features

### Parallel Processing
The application automatically utilizes multiple CPU cores to process files in parallel:
- Automatically detects and uses available CPU cores
- Splits file processing into chunks for concurrent execution
- Maintains thread safety for file operations and progress tracking
- Efficient resource utilization with worker pool pattern

### Progress Tracking
Real-time progress monitoring with visual feedback:
- Dynamic progress bar showing completion percentage
- Updates in-place without cluttering the console
- Percentage updates every 1% of completion
- Color-coded output for better visibility
- File-level progress tracking with skip support

## Data Structure

The application creates a trie-based directory structure where:
- Each character in the ID_BB_GLOBAL becomes a subdirectory
- The leaf directory contains a JSON file with the asset data
- A history.json file tracks property changes and their sources (when enabled)

## File Format

### Asset Data JSON
```json
{
  "ID_BB_GLOBAL": "BBG001234567",
  "ROWNUMBER": 123,
  "FILEDATE": 20250504,
  "ID_BB_YELLOWKEY": "123456",
  "TICKER_AND_EXCH_CODE": "AAPL US",
  "NAME": "Apple Inc",
  "ID_BB_COMPANY": 12345,
  "ID_BB_UNIQUE": "BBG001234567"
}
```

### History JSON
```json
{
  "propertyName": {
    "YYYYMMDD": {
      "file": "filename.csv.gz",
      "value": "value"
    }
  }
}
```

## Console Output

The application provides colorful console output to help track progress and identify important information:

- Info messages: Cyan
- Success messages: Green
- Warning messages: Yellow
- Error messages: Red
- Debug messages: Magenta
- File paths: Bright Blue
- IDs: Bright Green
- Dates: Bright Yellow
- Values: Bright White (Bold)
- Progress bar: Bright Green with visual blocks

## Performance Considerations

- The application automatically scales to use available CPU cores
- Large files are processed in chunks to optimize memory usage
- Progress tracking is synchronized to prevent display issues
- File operations are thread-safe to prevent data corruption
- Null values are automatically filtered out to reduce storage
- Processing can be resumed from any point using the skip feature
