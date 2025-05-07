package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type DataProcessor struct {
	config   Config
	metadata []Metadata
	logger   *Logger
}

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
	p.logger.Debug("Finding most recent file for %s", p.logger.HighlightFile(filename))
	filePath, err := p.findMostRecentFile(filename)
	if err != nil {
		return err
	}
	p.logger.Info("Found most recent file: %s", p.logger.HighlightFile(filePath))

	// Read and process the CSV file
	p.logger.Debug("Reading CSV file: %s", p.logger.HighlightFile(filePath))
	records, err := p.readCSVFile(filePath)
	if err != nil {
		return err
	}
	p.logger.Info("Read %d records from %s", len(records), p.logger.HighlightFile(filePath))

	// Process each record and store in trie structure
	for i, record := range records {
		if err := p.processRecord(record, filePath); err != nil {
			return err
		}
		if (i+1)%1000 == 0 {
			p.logger.Info("Processed %d records", i+1)
		}
	}

	return nil
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

func (p *DataProcessor) processRecord(record []string, sourceFile string) error {
	// Assuming ID_BB_GLOBAL is the first column
	id := record[0]
	if id == "" {
		return nil // Skip records without ID
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

	existingDataPath := filepath.Join(triePath, id+".json")
	if data, err := os.ReadFile(existingDataPath); err == nil {
		if err := json.Unmarshal(data, &assetData); err != nil {
			return err
		}
		p.logger.Debug("Loaded existing data for ID: %s", p.logger.HighlightID(id))
	}

	// Update properties
	for i, value := range record {
		propertyName := p.getColumnName(i)
		if propertyName == "" {
			continue
		}

		// Update property value
		assetData.Properties[propertyName] = value

		// Update property history
		if err := p.updatePropertyHistory(id, propertyName, value, sourceFile); err != nil {
			return err
		}
	}

	// Save updated data
	data, err := json.MarshalIndent(assetData, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(existingDataPath, data, 0644); err != nil {
		return err
	}

	p.logger.Debug("Updated data for ID: %s", p.logger.HighlightID(id))
	return nil
}

func (p *DataProcessor) createTriePath(id string) string {
	path := p.config.AssetDestRoot
	for _, char := range id {
		path = filepath.Join(path, string(char))
	}
	return path
}

func (p *DataProcessor) getColumnName(index int) string {
	// This should be implemented based on your metadata structure
	// For now, returning a placeholder
	return fmt.Sprintf("column_%d", index)
}

func (p *DataProcessor) updatePropertyHistory(id, propertyName, value, sourceFile string) error {
	historyPath := filepath.Join(p.createTriePath(id), "history.json")
	
	var history PropertyHistory
	if data, err := os.ReadFile(historyPath); err == nil {
		if err := json.Unmarshal(data, &history); err != nil {
			return err
		}
	}

	effectiveDate := p.getEffectiveDate(sourceFile)
	// Add new history entry
	history.History = append(history.History, History{
		Value:         value,
		SourceFile:    sourceFile,
		EffectiveDate: effectiveDate,
	})

	// Save updated history
	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(historyPath, data, 0644); err != nil {
		return err
	}

	p.logger.Debug("Updated history for ID: %s, Property: %s, Date: %s",
		p.logger.HighlightID(id),
		p.logger.HighlightValue(propertyName),
		p.logger.HighlightDate(effectiveDate))
	return nil
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