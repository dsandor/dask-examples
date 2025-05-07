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
	"sync"
	"runtime"
)

// RecordChunk represents a chunk of records to be processed
type RecordChunk struct {
	Records       [][]string
	Headers       []string
	ColumnMetadata map[string]Column
	SourceFile    string
	StartIndex    int
}

type DataProcessor struct {
	config   Config
	metadata []Metadata
	logger   *Logger
	wg       sync.WaitGroup
	mu       sync.Mutex
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

	// Calculate number of workers (use number of CPU cores)
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Create channels for work distribution
	chunkChan := make(chan RecordChunk, numWorkers)
	errorChan := make(chan error, numWorkers)

	// Start worker goroutines
	p.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go p.worker(chunkChan, errorChan)
	}

	// Calculate chunk size
	chunkSize := (len(records) - 1) / numWorkers
	if chunkSize < 1 {
		chunkSize = 1
	}

	// Split records into chunks and send to workers
	for i := 1; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}

		chunk := RecordChunk{
			Records:       records[i:end],
			Headers:       headers,
			ColumnMetadata: columnMetadata,
			SourceFile:    filePath,
			StartIndex:    i,
		}
		chunkChan <- chunk
	}
	close(chunkChan)

	// Wait for all workers to complete
	p.wg.Wait()
	close(errorChan)

	// Check for any errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	// Clear the progress bar and show completion
	p.logger.ClearProgress()
	p.logger.Success("Completed processing %s", p.logger.HighlightFile(filePath))

	return nil
}

func (p *DataProcessor) worker(chunkChan <-chan RecordChunk, errorChan chan<- error) {
	defer p.wg.Done()

	for chunk := range chunkChan {
		for i, record := range chunk.Records {
			if err := p.processRecord(record, chunk.Headers, chunk.ColumnMetadata, chunk.SourceFile); err != nil {
				errorChan <- err
				return
			}

			// Update progress every 1%
			globalIndex := chunk.StartIndex + i
			if p.config.EnableHistory && globalIndex%(p.getExpectedRowCount(filepath.Base(chunk.SourceFile))/100) == 0 {
				percentage := float64(globalIndex) / float64(p.getExpectedRowCount(filepath.Base(chunk.SourceFile))) * 100
				p.mu.Lock()
				p.logger.ProgressBar(chunk.SourceFile, percentage)
				p.mu.Unlock()
			}
		}
	}
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
	assetData := make(AssetData)
	assetData["ID_BB_GLOBAL"] = id
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

		// Update property value directly in the root object
		assetData[columnName] = convertedValue

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