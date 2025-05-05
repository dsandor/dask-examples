package analyzer

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileMetadata represents the metadata for a single file (same as in extractor package)
type FileMetadata struct {
	Filename     string       `json:"filename"`
	RelativePath string       `json:"relativePath"`
	EffectiveDate string      `json:"effectiveDate"`
	RowCount     int          `json:"rowCount"`
	Columns      []ColumnInfo `json:"columns"`
}

// ColumnInfo represents metadata for a single column (same as in extractor package)
type ColumnInfo struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

// AnalysisResult represents the result of the metadata analysis
type AnalysisResult struct {
	AssetFiles   []string `json:"assetFiles"`   // Files with ID_BB_GLOBAL column
	CompanyFiles []string `json:"companyFiles"` // Files with ID_BB_COMPANY but no ID_BB_GLOBAL
	Exceptions   []string `json:"exceptions"`   // Files with neither ID_BB_GLOBAL nor ID_BB_COMPANY
}

// AnalyzeMetadata analyzes the metadata JSON file and categorizes files
func AnalyzeMetadata(metadataFilePath string) (*AnalysisResult, error) {
	// Read the metadata file
	file, err := os.Open(metadataFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening metadata file: %w", err)
	}
	defer file.Close()

	// Read the file content
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error reading metadata file: %w", err)
	}

	// Parse the JSON
	var metadata []FileMetadata
	if err := json.Unmarshal(content, &metadata); err != nil {
		return nil, fmt.Errorf("error parsing metadata JSON: %w", err)
	}

	// Initialize result
	result := &AnalysisResult{
		AssetFiles:   make([]string, 0),
		CompanyFiles: make([]string, 0),
		Exceptions:   make([]string, 0),
	}

	// Analyze each file
	for _, file := range metadata {
		hasGlobalID := false
		hasCompanyID := false

		// Check for ID_BB_GLOBAL and ID_BB_COMPANY columns
		for _, column := range file.Columns {
			if column.Name == "ID_BB_GLOBAL" {
				hasGlobalID = true
			}
			if column.Name == "ID_BB_COMPANY" {
				hasCompanyID = true
			}
		}

		// Categorize the file
		if hasGlobalID {
			result.AssetFiles = append(result.AssetFiles, file.Filename)
		} else if hasCompanyID {
			result.CompanyFiles = append(result.CompanyFiles, file.Filename)
		} else {
			result.Exceptions = append(result.Exceptions, file.Filename)
		}
	}

	return result, nil
}

// WriteAnalysisToFile writes the analysis result to a JSON file
func WriteAnalysisToFile(result *AnalysisResult, outputPath string) error {
	// Marshal the result to JSON
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling analysis result to JSON: %w", err)
	}

	// Create the output directory if it doesn't exist
	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("error creating output directory: %w", err)
	}

	// Write the JSON to file
	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("error writing analysis result to file: %w", err)
	}

	return nil
}
