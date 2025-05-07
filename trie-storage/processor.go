package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type DataProcessor struct {
	config   Config
	metadata []Metadata
	logger   *Logger
}

// HistoryEntry represents a single history entry with file and value
type HistoryEntry struct {
	File  string      `json:"file"`
	Value interface{} `json:"value"`
}

// HistoryData represents the history data structure
type HistoryData map[string]map[string]HistoryEntry

func NewDataProcessor(config Config, metadata []Metadata, logger *Logger) *DataProcessor {
	return &DataProcessor{
		config:   config,
		metadata: metadata,
		logger:   logger,
	}
}

func (p *DataProcessor) ProcessAssetFiles(assetFiles []string) error {
	for _, filename := range assetFiles {
		p.logger.Info("Processing asset file: %s", p.logger.HighlightFile(filename))
		if err := p.processAssetFile(filename); err != nil {
			return fmt.Errorf("error processing asset file %s: %v", filename, err)
		}
		p.logger.Success("Completed processing asset file: %s", p.logger.HighlightFile(filename))
	}
	return nil
}

func (p *DataProcessor) processAssetFile(filename string) error {
	// Find the most recent file for this asset type
	filePath, err := p.findMostRecentFile(filename)
	if err != nil {
		return err
	}
	p.logger.Info("Found most recent file: %s", p.logger.HighlightFile(filePath))

	// Get expected row count from metadata
	expectedRows := p.getExpectedRowCount(filename)
	if expectedRows == 0 {
		p.logger.Warning("No row count found in metadata for %s", filename)
	}

	// Read and process the CSV file
	records, err := p.readCSVFile(filePath)
	if err != nil {
		return err
	}
	p.logger.Info("Read %d records from %s", len(records), p.logger.HighlightFile(filePath))

	// Get column headers and metadata
	headers := records[0]
	fileMetadata := p.getFileMetadata(filename)
	if fileMetadata == nil {
		return fmt.Errorf("no metadata found for file %s", filename)
	}

	// Create column index to metadata mapping
	columnMetadata := make(map[string]Column)
	for _, col := range fileMetadata.Columns {
		columnMetadata[col.Name] = col
	}

	// Process each record and store in trie structure
	for i, record := range records[1:] { // Skip header row
		if err := p.processRecord(record, headers, columnMetadata, filePath); err != nil {
			return err
		}

		// Log progress every 5%
		if expectedRows > 0 && (i+1)%(expectedRows/20) == 0 {
			percentage := float64(i+1) / float64(expectedRows) * 100
			p.logger.Info("Processing %s: %.1f%% complete", p.logger.HighlightFile(filePath), percentage)
		}
	}

	return nil
}

func (p *DataProcessor) getFileMetadata(filename string) *Metadata {
	for _, meta := range p.metadata {
		if meta.Filename == filename {
			return &meta
		}
	}
	return nil
}

func (p *DataProcessor) getExpectedRowCount(filename string) int {
	for _, meta := range p.metadata {
		if meta.Filename == filename {
			return meta.RowCount
		}
	}
	return 0
}

func (p *DataProcessor) findMostRecentFile(filename string) (string, error) {
	dir := filepath.Join(p.config.SourceRoot, filename)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".csv.gz") {
			files = append(files, entry.Name())
		}
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no CSV files found in directory %s", dir)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	return filepath.Join(dir, files[0]), nil
}

func (p *DataProcessor) readCSVFile(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	reader := csv.NewReader(gzReader)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (p *DataProcessor) processRecord(record []string, headers []string, columnMetadata map[string]Column, sourceFile string) error {
	// Find ID_BB_GLOBAL column index
	idIndex := -1
	for i, header := range headers {
		if header == "ID_BB_GLOBAL" {
			idIndex = i
			break
		}
	}

	if idIndex == -1 {
		return fmt.Errorf("ID_BB_GLOBAL column not found in headers")
	}

	id := record[idIndex]
	if id == "" || !strings.HasPrefix(id, "BBG") {
		return nil // Skip records without valid ID_BB_GLOBAL
	}

	// Create trie directory structure
	triePath := p.createTriePath(id)
	if err := os.MkdirAll(triePath, 0755); err != nil {
		return err
	}

	// Load existing data if any
	assetData := AssetData{
		ID_BB_GLOBAL: id,
		Properties:   make(map[string]interface{}),
	}

	existingDataPath := filepath.Join(triePath, "data.json")
	if data, err := os.ReadFile(existingDataPath); err == nil {
		if err := json.Unmarshal(data, &assetData); err != nil {
			return err
		}
	}

	// Get effective date from source file
	effectiveDate := p.getEffectiveDate(sourceFile)
	if effectiveDate == "" {
		p.logger.Warning("Could not extract effective date from file: %s", sourceFile)
		return nil
	}

	// Update properties and history
	for i, value := range record {
		columnName := headers[i]
		metadata, exists := columnMetadata[columnName]
		if !exists {
			continue
		}

		// Convert value based on data type
		convertedValue, err := p.convertValue(value, metadata.DataType)
		if err != nil {
			p.logger.Warning("Failed to convert value for column %s: %v", columnName, err)
			continue
		}

		// Skip null values
		if convertedValue == nil {
			continue
		}

		// Update property value
		assetData.Properties[columnName] = convertedValue

		// Update history if enabled
		if p.config.EnableHistory {
			if err := p.updatePropertyHistory(id, columnName, convertedValue, sourceFile, effectiveDate); err != nil {
				return err
			}
		}
	}

	// Save updated data
	data, err := json.MarshalIndent(assetData, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(existingDataPath, data, 0644)
}

func (p *DataProcessor) convertValue(value string, dataType string) (interface{}, error) {
	// Check for null-like values
	if p.isNullValue(value) {
		return nil, nil
	}

	switch dataType {
	case "integer":
		return strconv.Atoi(value)
	case "text":
		return value, nil
	case "float":
		return strconv.ParseFloat(value, 64)
	default:
		return value, nil
	}
}

// isNullValue checks if a value should be treated as null
func (p *DataProcessor) isNullValue(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || 
		strings.EqualFold(value, "N.A.") || 
		strings.EqualFold(value, "n.a.") || 
		strings.EqualFold(value, "null") ||
		strings.EqualFold(value, "nil") ||
		strings.EqualFold(value, "none") ||
		strings.EqualFold(value, "N/A") ||
		strings.EqualFold(value, "n/a")
}

func (p *DataProcessor) createTriePath(id string) string {
	path := p.config.AssetDestRoot
	for _, char := range id {
		path = filepath.Join(path, string(char))
	}
	return path
}

func (p *DataProcessor) updatePropertyHistory(id, propertyName string, value interface{}, sourceFile, effectiveDate string) error {
	historyPath := filepath.Join(p.createTriePath(id), "history.json")
	
	var history HistoryData
	if data, err := os.ReadFile(historyPath); err == nil {
		if err := json.Unmarshal(data, &history); err != nil {
			return err
		}
	} else {
		history = make(HistoryData)
	}

	// Initialize property map if it doesn't exist
	if _, exists := history[propertyName]; !exists {
		history[propertyName] = make(map[string]HistoryEntry)
	}

	// Add or update history entry
	history[propertyName][effectiveDate] = HistoryEntry{
		File:  filepath.Base(sourceFile),
		Value: value,
	}

	// Save updated history
	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(historyPath, data, 0644)
}

func (p *DataProcessor) getEffectiveDate(filePath string) string {
	// Extract date from filename (YYYYMMDD format)
	base := filepath.Base(filePath)
	parts := strings.Split(base, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-2]
	}
	return ""
} 