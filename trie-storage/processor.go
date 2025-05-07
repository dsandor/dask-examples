package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	Records       [][]string
	Headers       []string
	ColumnMetadata map[string]Column
	SourceFile    string
	StartIndex    int
}

type DataProcessor struct {
	config     *Config
	logger     *Logger
	tui        *TUI
	mu         sync.Mutex
}

// HistoryEntry represents a single history entry with file and value
type HistoryEntry struct {
	File  string      `json:"file"`
	Value interface{} `json:"value"`
}

// HistoryData represents the history data structure
type HistoryData map[string]map[string]HistoryEntry

func NewDataProcessor(config *Config) *DataProcessor {
	return &DataProcessor{
		config: config,
		logger: NewLogger(),
		tui:    NewTUI(runtime.NumCPU()),
	}
}

func (p *DataProcessor) ProcessAssetFiles(files []string) error {
	totalFiles := len(files)
	processedFiles := 0
	skippedFiles := 0

	for _, filename := range files {
		// Find the most recent data file in the directory
		fullPath, err := p.findMostRecentFile(filename)
		if err != nil {
			return fmt.Errorf("error finding most recent file for %s: %v", filename, err)
		}

		if skippedFiles < p.config.SkipFiles {
			p.logger.Info("Skipping file %d/%d: %s", 
				skippedFiles+1, 
				p.config.SkipFiles,
				p.logger.HighlightFile(fullPath))
			skippedFiles++
			continue
		}

		processedFiles++
		p.logger.Info("Processing file %d/%d: %s", 
			processedFiles, 
			totalFiles-p.config.SkipFiles,
			p.logger.HighlightFile(fullPath))

		if err := p.processAssetFile(fullPath); err != nil {
			return fmt.Errorf("error processing asset file %s: %v", filename, err)
		}
		p.logger.Success("Completed processing file %d/%d: %s", 
			processedFiles, 
			totalFiles-p.config.SkipFiles,
			p.logger.HighlightFile(fullPath))
	}
	return nil
}

func (p *DataProcessor) processAssetFile(filePath string) error {
	startTime := time.Now()
	p.logger.Info("Processing %s", p.logger.HighlightFile(filePath))

	// Start TUI
	p.tui.Start()
	defer p.tui.Stop()

	// Read and parse the file
	readStart := time.Now()
	records, err := p.readCSVFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}
	readDuration := time.Since(readStart)
	p.logger.Info("Read %d records from %s in %.2fs", len(records), p.logger.HighlightFile(filePath), readDuration.Seconds())

	// Process metadata
	metaStart := time.Now()
	metadata, err := p.processMetadata(records)
	if err != nil {
		return fmt.Errorf("error processing metadata: %v", err)
	}
	metaDuration := time.Since(metaStart)
	p.logger.Info("Processed metadata in %.2fs", metaDuration.Seconds())

	// Prepare chunks for parallel processing
	chunkStart := time.Now()
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}
	chunkSize := (len(records) + numWorkers - 1) / numWorkers
	chunks := make([]RecordChunk, numWorkers)
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunks[i] = RecordChunk{
			Records:       records[start:end],
			Headers:       records[0],
			ColumnMetadata: metadata,
			SourceFile:    filePath,
			StartIndex:    start,
		}
	}
	chunkDuration := time.Since(chunkStart)
	p.logger.Info("Prepared %d chunks for parallel processing in %.2fs", numWorkers, chunkDuration.Seconds())

	// Create channels for work distribution
	chunkChan := make(chan RecordChunk, numWorkers)
	errChan := make(chan error, numWorkers)

	// Start worker goroutines
	workerStart := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for chunk := range chunkChan {
				processed := 0
				total := len(chunk.Records)
				for _, record := range chunk.Records {
					if err := p.processRecord(record, chunk.Headers, chunk.ColumnMetadata, chunk.SourceFile); err != nil {
						errChan <- fmt.Errorf("error processing record: %v", err)
						return
					}
					processed++
					// Report progress every 1%
					if processed%(total/100+1) == 0 {
						progress := float64(processed) * 100 / float64(total)
						p.tui.UpdateProgress(workerID, progress, fmt.Sprintf("Processing %d/%d records", processed, total))
					}
				}
			}
		}(i)
	}
	workerDuration := time.Since(workerStart)
	p.logger.Info("Started %d worker goroutines in %.3fs", numWorkers, workerDuration.Seconds())

	// Send chunks to workers
	sendStart := time.Now()
	p.logger.Info("Sending %d chunks to workers...", numWorkers)
	for _, chunk := range chunks {
		chunkChan <- chunk
	}
	close(chunkChan)
	sendDuration := time.Since(sendStart)
	p.logger.Info("Sent all chunks to workers in %.3fs", sendDuration.Seconds())

	// Wait for all workers to complete
	p.logger.Info("Waiting for workers to complete processing...")
	waitStart := time.Now()
	wg.Wait()
	waitDuration := time.Since(waitStart)
	p.logger.Info("All workers completed in %.2fs", waitDuration.Seconds())

	// Check for errors
	checkStart := time.Now()
	close(errChan)
	if err := <-errChan; err != nil {
		return err
	}
	checkDuration := time.Since(checkStart)
	p.logger.Info("Error check completed in %.3fs", checkDuration.Seconds())

	// Log completion
	totalDuration := time.Since(startTime)
	p.logger.Success("Completed processing %s in %.1fs (Read: %.2fs, Setup: %.2fs, Process: %.2fs)",
		p.logger.HighlightFile(filePath),
		totalDuration.Seconds(),
		readDuration.Seconds(),
		metaDuration.Seconds(),
		waitDuration.Seconds())

	return nil
}

func (p *DataProcessor) processMetadata(records [][]string) (map[string]Column, error) {
	if len(records) < 2 {
		return nil, fmt.Errorf("file must contain at least a header row and one data row")
	}

	metadata := make(map[string]Column)

	// Find the metadata for this file
	var fileMetadata *Metadata
	for _, meta := range p.config.Metadata {
		if meta.Filename == filepath.Base(records[0][0]) {
			fileMetadata = &meta
			break
		}
	}

	if fileMetadata == nil {
		return nil, fmt.Errorf("no metadata found for file")
	}

	// Create column metadata mapping
	for _, col := range fileMetadata.Columns {
		metadata[col.Name] = col
	}

	return metadata, nil
}

func (p *DataProcessor) getFileMetadata(filename string) *Metadata {
	for _, meta := range p.config.Metadata {
		if meta.Filename == filename {
			return &meta
		}
	}
	return nil
}

func (p *DataProcessor) getExpectedRowCount(filename string) int {
	for _, meta := range p.config.Metadata {
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