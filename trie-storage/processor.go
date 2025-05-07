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
	"time"
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
	totalFiles := len(assetFiles)
	processedFiles := 0
	skippedFiles := 0

	for _, filename := range assetFiles {
		if skippedFiles < p.config.SkipFiles {
			p.logger.Info("Skipping file %d/%d: %s", 
				skippedFiles+1, 
				p.config.SkipFiles,
				p.logger.HighlightFile(filename))
			skippedFiles++
			continue
		}

		processedFiles++
		p.logger.Info("Processing file %d/%d: %s", 
			processedFiles, 
			totalFiles-p.config.SkipFiles,
			p.logger.HighlightFile(filename))

		if err := p.processAssetFile(filename); err != nil {
			return fmt.Errorf("error processing asset file %s: %v", filename, err)
		}
		p.logger.Success("Completed processing file %d/%d: %s", 
			processedFiles, 
			totalFiles-p.config.SkipFiles,
			p.logger.HighlightFile(filename))
	}
	return nil
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

	// Read and process the CSV file
	readStart := time.Now()
	records, err := p.readCSVFile(filePath)
	if err != nil {
		return err
	}
	readDuration := time.Since(readStart)
	p.logger.Info("Read %d records from %s in %s", 
		len(records), 
		p.logger.HighlightFile(filePath),
		p.logger.HighlightValue(readDuration))

	// Get column headers and metadata
	metaStart := time.Now()
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
	metaDuration := time.Since(metaStart)
	p.logger.Info("Processed metadata in %s", p.logger.HighlightValue(metaDuration))

	// Calculate number of workers (use number of CPU cores)
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Calculate chunk size
	chunkSize := (len(records) - 1) / numWorkers
	if chunkSize < 1 {
		chunkSize = 1
	}

	// Pre-allocate chunks to avoid allocations during processing
	chunks := make([]RecordChunk, 0, numWorkers)
	for i := 1; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}

		chunks = append(chunks, RecordChunk{
			Records:       records[i:end],
			Headers:       headers,
			ColumnMetadata: columnMetadata,
			SourceFile:    filePath,
			StartIndex:    i,
		})
	}
	chunkDuration := time.Since(metaStart)
	p.logger.Info("Prepared %d chunks for parallel processing in %s", 
		len(chunks), 
		p.logger.HighlightValue(chunkDuration))

	// Create channels for work distribution
	chunkChan := make(chan RecordChunk, numWorkers)
	errorChan := make(chan error, numWorkers)
	progressChan := make(chan int, numWorkers*2) // Buffer for progress updates

	// Start worker goroutines
	p.wg.Add(numWorkers)
	processStart := time.Now()
	p.logger.Info("Starting parallel processing with %s worker threads", p.logger.HighlightValue(numWorkers))

	// Start workers
	workerStart := time.Now()
	for i := 0; i < numWorkers; i++ {
		go p.worker(chunkChan, errorChan, progressChan)
	}
	workerDuration := time.Since(workerStart)
	p.logger.Info("Started %d worker goroutines in %s", p.logger.HighlightValue(numWorkers), p.logger.HighlightValue(workerDuration))

	// Start progress monitor
	monitorStart := time.Now()
	go p.monitorProgress(expectedRows, filePath, progressChan)
	monitorDuration := time.Since(monitorStart)
	p.logger.Info("Started progress monitor in %s", p.logger.HighlightValue(monitorDuration))

	// Send chunks to workers
	chunkSendStart := time.Now()
	p.logger.Info("Sending %d chunks to workers...", p.logger.HighlightValue(len(chunks)))
	for _, chunk := range chunks {
		chunkChan <- chunk
	}
	close(chunkChan)
	chunkSendDuration := time.Since(chunkSendStart)
	p.logger.Info("Sent all chunks to workers in %s", p.logger.HighlightValue(chunkSendDuration))

	// Wait for all workers to complete
	waitStart := time.Now()
	p.logger.Info("Waiting for workers to complete processing...")
	p.wg.Wait()
	waitDuration := time.Since(waitStart)
	p.logger.Info("All workers completed in %s", p.logger.HighlightValue(waitDuration))

	close(errorChan)
	close(progressChan)

	// Check for any errors
	errorCheckStart := time.Now()
	for err := range errorChan {
		if err != nil {
			return err
		}
	}
	errorCheckDuration := time.Since(errorCheckStart)
	p.logger.Info("Error check completed in %s", p.logger.HighlightValue(errorCheckDuration))

	processDuration := time.Since(processStart)
	totalDuration := time.Since(startTime)

	// Clear the progress bar and show completion
	p.logger.ClearProgress()
	p.logger.Success("Completed processing %s in %s (Read: %s, Setup: %s, Process: %s)", 
		p.logger.HighlightFile(filePath),
		p.logger.HighlightValue(totalDuration),
		p.logger.HighlightValue(readDuration),
		p.logger.HighlightValue(chunkDuration),
		p.logger.HighlightValue(processDuration))

	return nil
}

func (p *DataProcessor) monitorProgress(expectedRows int, filePath string, progressChan <-chan int) {
	processedRows := 0
	lastPercentage := 0

	for count := range progressChan {
		processedRows += count
		if expectedRows > 0 {
			percentage := int(float64(processedRows) / float64(expectedRows) * 100)
			if percentage > lastPercentage {
				lastPercentage = percentage
				p.mu.Lock()
				p.logger.ProgressBar(filePath, float64(percentage))
				p.mu.Unlock()
			}
		}
	}
}

func (p *DataProcessor) worker(chunkChan <-chan RecordChunk, errorChan chan<- error, progressChan chan<- int) {
	defer p.wg.Done()

	for chunk := range chunkChan {
		processedCount := 0
		for _, record := range chunk.Records {
			if err := p.processRecord(record, chunk.Headers, chunk.ColumnMetadata, chunk.SourceFile); err != nil {
				errorChan <- err
				return
			}
			processedCount++
		}
		progressChan <- processedCount
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