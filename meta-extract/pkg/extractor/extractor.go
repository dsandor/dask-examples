package extractor

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// FileMetadata represents the metadata for a single file
type FileMetadata struct {
	Filename     string       `json:"filename"`
	RelativePath string       `json:"relativePath"`
	EffectiveDate string      `json:"effectiveDate"`
	RowCount     int          `json:"rowCount"`
	Columns      []ColumnInfo `json:"columns"`
}

// ColumnInfo represents metadata for a single column
type ColumnInfo struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// ExtractMetadata extracts metadata from all files in the given root path
func ExtractMetadata(rootPath string) ([]FileMetadata, error) {
	var metadata []FileMetadata

	// Walk through all directories and files
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if path == rootPath {
			return nil
		}

		// Check if it's a file with .gz extension
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".gz") {
			// Get relative path from root
			relPath, err := filepath.Rel(rootPath, path)
			if err != nil {
				return fmt.Errorf("error getting relative path: %w", err)
			}

			// Extract directory name (which is the file name)
			dirName := filepath.Base(filepath.Dir(path))

			// Extract effective date from filename
			effectiveDate, err := extractEffectiveDate(info.Name())
			if err != nil {
				return fmt.Errorf("error extracting effective date from %s: %w", info.Name(), err)
			}

			// Process the file
			fileMetadata, err := processFile(path, dirName, relPath, effectiveDate)
			if err != nil {
				return fmt.Errorf("error processing file %s: %w", path, err)
			}

			metadata = append(metadata, fileMetadata)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	return metadata, nil
}

// extractEffectiveDate extracts the YYYYMMDD date from a filename
func extractEffectiveDate(filename string) (string, error) {
	// Regular expression to match YYYYMMDD format in the filename
	re := regexp.MustCompile(`\d{8}`)
	match := re.FindString(filename)
	
	if match == "" {
		return "", fmt.Errorf("no date in YYYYMMDD format found in filename: %s", filename)
	}
	
	// Validate the date
	_, err := time.Parse("20060102", match)
	if err != nil {
		return "", fmt.Errorf("invalid date format in filename: %s", filename)
	}
	
	return match, nil
}

// processFile extracts metadata from a single gzipped CSV file
func processFile(filePath, filename, relativePath, effectiveDate string) (FileMetadata, error) {
	// Open the gzipped file
	file, err := os.Open(filePath)
	if err != nil {
		return FileMetadata{}, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return FileMetadata{}, fmt.Errorf("error creating gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Create a CSV reader
	csvReader := csv.NewReader(gzipReader)

	// Read the header row to get column names
	header, err := csvReader.Read()
	if err != nil {
		return FileMetadata{}, fmt.Errorf("error reading CSV header: %w", err)
	}

	// Initialize column data for type inference
	columnData := make([][]string, len(header))
	for i := range columnData {
		columnData[i] = make([]string, 0, 100) // Pre-allocate for efficiency
	}

	// Read rows to count and sample data for type inference
	rowCount := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return FileMetadata{}, fmt.Errorf("error reading CSV row: %w", err)
		}

		rowCount++

		// Sample data for type inference (up to 100 rows)
		if rowCount <= 100 {
			for i, value := range row {
				if i < len(columnData) {
					columnData[i] = append(columnData[i], value)
				}
			}
		}
	}

	// Create column info with inferred types
	columns := make([]ColumnInfo, len(header))
	for i, name := range header {
		dataType := inferPostgresType(columnData[i])
		columns[i] = ColumnInfo{
			Name:     name,
			DataType: dataType,
		}
	}

	return FileMetadata{
		Filename:     filename,
		RelativePath: relativePath,
		EffectiveDate: effectiveDate,
		RowCount:     rowCount,
		Columns:      columns,
	}, nil
}

// inferPostgresType infers the PostgreSQL data type from a sample of values
func inferPostgresType(values []string) string {
	if len(values) == 0 {
		return "text"
	}

	// Check if all values are empty or null
	allEmpty := true
	for _, v := range values {
		if v != "" && strings.ToLower(v) != "null" && strings.ToLower(v) != "nil" {
			allEmpty = false
			break
		}
	}
	if allEmpty {
		return "text"
	}

	// Try to infer the type based on the values
	isInteger := true
	isNumeric := true
	isDate := true
	isTimestamp := true
	isBoolean := true

	for _, v := range values {
		// Skip empty values for type inference
		if v == "" || strings.ToLower(v) == "null" || strings.ToLower(v) == "nil" {
			continue
		}

		// Check if value is an integer
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			isInteger = false
		}

		// Check if value is numeric
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			isNumeric = false
		}

		// Check if value is a date (YYYY-MM-DD)
		if _, err := time.Parse("2006-01-02", v); err != nil {
			isDate = false
		}

		// Check if value is a timestamp
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			// Try another common timestamp format
			if _, err := time.Parse("2006-01-02 15:04:05", v); err != nil {
				isTimestamp = false
			}
		}

		// Check if value is a boolean
		lowerV := strings.ToLower(v)
		if lowerV != "true" && lowerV != "false" && lowerV != "t" && lowerV != "f" && 
		   lowerV != "yes" && lowerV != "no" && lowerV != "y" && lowerV != "n" && 
		   lowerV != "1" && lowerV != "0" {
			isBoolean = false
		}
	}

	// Determine the most specific type that fits all values
	if isInteger {
		return "integer"
	}
	if isNumeric {
		return "numeric"
	}
	if isDate {
		return "date"
	}
	if isTimestamp {
		return "timestamp"
	}
	if isBoolean {
		return "boolean"
	}

	// Default to text
	return "text"
}
