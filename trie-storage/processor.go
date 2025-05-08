package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RecordChunk represents a chunk of records to be processed
type RecordChunk struct {
	Records        [][]string
	Headers        []string
	ColumnMetadata map[string]Column
	SourceFile     string
	StartIndex     int
}

// TypeMismatch tracks columns that need their data type updated in metadata
type TypeMismatch struct {
	Filename   string
	ColumnName string
	ActualType string
	mu         sync.Mutex
}

type DataProcessor struct {
	config         Config
	metadata       []Metadata
	logger         *Logger
	processedFiles *ProcessedFiles
	fileChan       chan string
	errorChan      chan error
	wg             sync.WaitGroup
	mu             sync.Mutex
	typeMismatches map[string]map[string]string // filename -> column -> actual type
}

// HistoryEntry represents a single history entry with file and value
type HistoryEntry struct {
	File  string      `json:"file"`
	Value interface{} `json:"value"`
}

// HistoryData represents the history data structure
type HistoryData map[string]map[string]HistoryEntry

// ProcessedFiles tracks which files have been successfully processed
type ProcessedFiles struct {
	mu    sync.Mutex
	files map[string]bool
	path  string
}

func NewProcessedFiles(checkpointPath string) (*ProcessedFiles, error) {
	pf := &ProcessedFiles{
		files: make(map[string]bool),
		path:  checkpointPath,
	}

	// Load existing checkpoint if it exists
	if data, err := os.ReadFile(checkpointPath); err == nil {
		if err := json.Unmarshal(data, &pf.files); err != nil {
			return nil, fmt.Errorf("error loading checkpoint: %v", err)
		}
	}

	return pf, nil
}

func NewDataProcessor(config Config, metadata []Metadata, logger *Logger) (*DataProcessor, error) {
	processedFiles, err := NewProcessedFiles(filepath.Join(config.AssetDestRoot, "processed_files.json"))
	if err != nil {
		return nil, fmt.Errorf("error initializing processed files tracking: %v", err)
	}

	return &DataProcessor{
		config:         config,
		metadata:       metadata,
		logger:         logger,
		processedFiles: processedFiles,
		fileChan:       make(chan string, runtime.NumCPU()),
		errorChan:      make(chan error, runtime.NumCPU()),
		typeMismatches: make(map[string]map[string]string),
	}, nil
}

func (p *DataProcessor) ProcessAssetFiles(assetFiles []string) error {
	totalFiles := len(assetFiles)
	processedFiles := 0
	skippedFiles := 0

	// Start worker goroutines
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	p.logger.Info("Starting %d worker goroutines", numWorkers)
	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go p.fileWorker()
	}

	// Send files to workers
	for _, filename := range assetFiles {
		if skippedFiles < p.config.SkipFiles {
			p.logger.Info("Skipping file %d/%d: %s",
				skippedFiles+1,
				p.config.SkipFiles,
				p.logger.HighlightFile(filename))
			skippedFiles++
			continue
		}

		// Skip already processed files
		if p.processedFiles.IsProcessed(filename) {
			p.logger.Info("Skipping already processed file: %s", p.logger.HighlightFile(filename))
			continue
		}

		processedFiles++
		p.logger.Info("Queueing file %d/%d: %s",
			processedFiles,
			totalFiles-p.config.SkipFiles,
			p.logger.HighlightFile(filename))

		p.fileChan <- filename
	}

	// Close channel after all files are queued
	close(p.fileChan)

	// Wait for all workers to complete
	p.logger.Info("Waiting for workers to complete processing...")
	p.wg.Wait()

	// Check for any errors
	close(p.errorChan)
	for err := range p.errorChan {
		if err != nil {
			return err
		}
	}

	// Update metadata with any type corrections
	if err := p.updateMetadata(); err != nil {
		p.logger.Error("Failed to update metadata: %v", err)
		return err
	}

	return nil
}

func (p *DataProcessor) fileWorker() {
	defer p.wg.Done()

	for filename := range p.fileChan {
		if err := p.processAssetFile(filename); err != nil {
			p.errorChan <- fmt.Errorf("error processing file %s: %v", filename, err)
			continue
		}

		// Mark file as processed only after successful completion
		if err := p.processedFiles.MarkProcessed(filename); err != nil {
			p.errorChan <- fmt.Errorf("error marking file %s as processed: %v", filename, err)
		}
	}
}

func (p *DataProcessor) processAssetFile(filename string) error {
	startTime := time.Now()

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

	// Open and prepare the file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	reader := csv.NewReader(gzReader)

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			p.logger.Warning("CSV file is empty: %s", filePath)
			return nil
		}
		return err
	}

	if len(headers) == 0 {
		p.logger.Warning("CSV file has no headers: %s", filePath)
		return nil
	}

	// Get file metadata
	fileMetadata := p.getFileMetadata(filename)
	if fileMetadata == nil {
		return fmt.Errorf("no metadata found for file %s", filename)
	}

	// Create column index to metadata mapping
	columnMetadata := make(map[string]Column)
	for _, col := range fileMetadata.Columns {
		columnMetadata[col.Name] = col
	}

	// Process records line by line
	processedRows := 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err := p.processRecord(record, headers, columnMetadata, filePath); err != nil {
			return err
		}

		processedRows++
		if expectedRows > 0 && processedRows%1000 == 0 {
			percentage := int(float64(processedRows) / float64(expectedRows) * 100)
			p.mu.Lock()
			p.logger.ProgressBar(filePath, float64(percentage))
			p.mu.Unlock()
		}
	}

	totalDuration := time.Since(startTime)
	p.logger.Success("Completed processing %s in %s (Processed %d records)",
		p.logger.HighlightFile(filePath),
		p.logger.HighlightValue(totalDuration),
		processedRows)

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

func (p *DataProcessor) getEffectiveDate(filePath string) string {
	// Extract date from filename (YYYYMMDD format)
	base := filepath.Base(filePath)
	parts := strings.Split(base, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-2]
	}
	return ""
}

func (pf *ProcessedFiles) IsProcessed(filename string) bool {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	return pf.files[filename]
}

func (pf *ProcessedFiles) MarkProcessed(filename string) error {
	pf.mu.Lock()
	pf.files[filename] = true
	pf.mu.Unlock()
	return pf.save()
}

func (pf *ProcessedFiles) save() error {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	data, err := json.MarshalIndent(pf.files, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(pf.path, data, 0644)
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
		convertedValue, err := p.convertValue(value, metadata.DataType, sourceFile, columnName)
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

func (p *DataProcessor) trackTypeMismatch(filename, columnName, actualType string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.typeMismatches[filename]; !exists {
		p.typeMismatches[filename] = make(map[string]string)
	}
	p.typeMismatches[filename][columnName] = actualType
}

func (p *DataProcessor) updateMetadata() error {
	if len(p.typeMismatches) == 0 {
		return nil
	}

	p.logger.Info("Updating metadata with corrected data types...")

	// Create a map of filename to metadata index for quick lookup
	metadataMap := make(map[string]int)
	for i, meta := range p.metadata {
		metadataMap[meta.Filename] = i
	}

	// Update metadata with corrected types
	for filename, columns := range p.typeMismatches {
		if idx, exists := metadataMap[filename]; exists {
			for colName, actualType := range columns {
				// Find and update the column in metadata
				for i, col := range p.metadata[idx].Columns {
					if col.Name == colName {
						p.logger.Info("Updating metadata for %s: column %s type from %s to %s",
							filename, colName, col.DataType, actualType)
						p.metadata[idx].Columns[i].DataType = actualType
						break
					}
				}
			}
		}
	}

	// Save updated metadata
	data, err := json.MarshalIndent(p.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling updated metadata: %v", err)
	}

	if err := os.WriteFile(p.config.MetadataFile, data, 0644); err != nil {
		return fmt.Errorf("error saving updated metadata: %v", err)
	}

	p.logger.Success("Successfully updated metadata with corrected data types")
	return nil
}

func (p *DataProcessor) convertValue(value string, dataType string, filename string, columnName string) (interface{}, error) {
	// Check for null-like values
	if p.isNullValue(value) {
		return nil, nil
	}

	// Try to infer the actual type if the metadata type doesn't match
	switch dataType {
	case "integer":
		// First try to parse as integer
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal, nil
		}
		// If it contains non-numeric characters, keep it as text
		if strings.ContainsAny(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_") {
			p.logger.Debug("Converting integer field to text due to non-numeric content: %s", value)
			p.trackTypeMismatch(filename, columnName, "text")
			return value, nil
		}
		// If it's a float, convert to integer
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return int(floatVal), nil
		}
		return nil, fmt.Errorf("failed to convert value to integer: %s", value)

	case "float":
		// Try to parse as float
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal, nil
		}
		// If it contains non-numeric characters, keep it as text
		if strings.ContainsAny(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_") {
			p.logger.Debug("Converting float field to text due to non-numeric content: %s", value)
			p.trackTypeMismatch(filename, columnName, "text")
			return value, nil
		}
		return nil, fmt.Errorf("failed to convert value to float: %s", value)

	case "text":
		return value, nil

	default:
		// For unknown types, try to infer the type
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal, nil
		}
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal, nil
		}
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
