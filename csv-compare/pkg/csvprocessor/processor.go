package csvprocessor

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"csv-compare/pkg/fileutils"

	"github.com/xuri/excelize/v2"
)

// DeltaRecord represents a change between two CSV records
type DeltaRecord struct {
	PrimaryKey string
	Changes    map[string]ColumnChange
}

// ColumnChange represents a change in a column value
type ColumnChange struct {
	Column      string      `json:"column"`
	PreviousVal interface{} `json:"previous_value"`
	CurrentVal  interface{} `json:"current_value"`
}

// CompareStats represents statistics about the comparison
type CompareStats struct {
	NewRecords     int `json:"newrecords"`
	RemovedRecords int `json:"removedrecords"`
}

// CSVProcessor handles CSV comparison operations
type CSVProcessor struct {
	PreviousFile      string
	CurrentFile       string
	Headers           []string
	headerMap         map[string]int
	IgnoredColumns    []string
	ignoredColumnsMap map[string]bool
	prevDataMutex     sync.Mutex
	PrimaryKey        string
	// Add summary statistics
	CurrentRowCount int
	DeltaRowCount   int
	ChangedColumns  map[string]bool
}

// NewCSVProcessor creates a new CSVProcessor instance
func NewCSVProcessor(previousFile, currentFile string, primaryKey string, ignoredColumns []string) *CSVProcessor {
	// Create map for faster lookups of ignored columns
	ignoredColumnsMap := make(map[string]bool)
	for _, col := range ignoredColumns {
		ignoredColumnsMap[col] = true
	}

	if len(ignoredColumns) > 0 {
		fileutils.LogInfo("The following columns will be ignored when determining differences:")
		for _, col := range ignoredColumns {
			fmt.Printf("  - %s\n", fileutils.Highlight(col))
		}
	}

	return &CSVProcessor{
		PreviousFile:      previousFile,
		CurrentFile:       currentFile,
		PrimaryKey:        primaryKey,
		headerMap:         make(map[string]int),
		IgnoredColumns:    ignoredColumns,
		ignoredColumnsMap: ignoredColumnsMap,
		ChangedColumns:    make(map[string]bool),
	}
}

// CompareCSVs compares two CSV files and writes the delta to output files
func (p *CSVProcessor) CompareCSVs(deltaCSVPath, changeLogPath string) error {
	fileutils.LogInfo("Starting comparison between files")

	// Open previous file with support for gzip
	prevReader, prevCloser, err := openCSVReader(p.PreviousFile)
	if err != nil {
		fileutils.LogError("Error opening previous file: %v", err)
		return fmt.Errorf("error opening previous file: %w", err)
	}
	defer prevCloser()

	// Open current file with support for gzip
	currReader, currCloser, err := openCSVReader(p.CurrentFile)
	if err != nil {
		fileutils.LogError("Error opening current file: %v", err)
		return fmt.Errorf("error opening current file: %w", err)
	}
	defer currCloser()

	// Read headers from both files
	prevHeaders, err := prevReader.Read()
	if err != nil {
		return fmt.Errorf("error reading previous file headers: %w", err)
	}

	currHeaders, err := currReader.Read()
	if err != nil {
		return fmt.Errorf("error reading current file headers: %w", err)
	}

	// Verify headers match
	if !headerMatch(prevHeaders, currHeaders) {
		return fmt.Errorf("headers do not match between files")
	}

	p.Headers = currHeaders
	for i, header := range p.Headers {
		p.headerMap[header] = i
	}

	// Create delta CSV file
	deltaFile, err := os.Create(deltaCSVPath)
	if err != nil {
		return fmt.Errorf("error creating delta file: %w", err)
	}
	defer deltaFile.Close()

	deltaWriter := csv.NewWriter(deltaFile)
	defer deltaWriter.Flush()

	// Write headers to delta file
	if err := deltaWriter.Write(p.Headers); err != nil {
		return fmt.Errorf("error writing headers to delta file: %w", err)
	}

	// Create changes log file
	changesFile, err := os.Create(changeLogPath)
	if err != nil {
		return fmt.Errorf("error creating changes log file: %w", err)
	}
	defer changesFile.Close()

	// Load previous data into memory for fast lookup
	prevData := make(map[string][]string)
	keyIndex := 0 // Assuming first column is the key

	for {
		record, err := prevReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading previous file: %w", err)
		}

		if len(record) > keyIndex {
			prevData[record[keyIndex]] = record
		}
	}

	// Process current file and compare with previous
	var changes []DeltaRecord
	rowIndex := 0

	for {
		currRecord, err := currReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading current file: %w", err)
		}

		rowIndex++

		if len(currRecord) <= keyIndex {
			continue
		}

		key := currRecord[keyIndex]
		prevRecord, exists := prevData[key]

		// If record doesn't exist in previous file or has changes, write to delta
		if !exists || !p.recordsMatch(prevRecord, currRecord) {
			if err := deltaWriter.Write(currRecord); err != nil {
				return fmt.Errorf("error writing to delta file: %w", err)
			}

			// If record exists in previous file, log changes
			if exists {
				deltaRecord := DeltaRecord{
					PrimaryKey: key,
					Changes:    make(map[string]ColumnChange),
				}

				for i, val := range currRecord {
					if i < len(prevRecord) && val != prevRecord[i] && i < len(p.Headers) {
						colName := p.Headers[i]
						deltaRecord.Changes[colName] = ColumnChange{
							Column:      colName,
							PreviousVal: prevRecord[i],
							CurrentVal:  val,
						}
					}
				}

				if len(deltaRecord.Changes) > 0 {
					changes = append(changes, deltaRecord)
				}
			}
		}

		// Remove processed record from prevData to identify records that exist only in previous file
		delete(prevData, key)
	}

	// Write changes to JSON file
	encoder := json.NewEncoder(changesFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(changes); err != nil {
		return fmt.Errorf("error writing changes to log file: %w", err)
	}

	return nil
}

// headerMatch checks if two header slices contain the same elements
func headerMatch(h1, h2 []string) bool {
	if len(h1) != len(h2) {
		return false
	}

	for i, v := range h1 {
		if v != h2[i] {
			return false
		}
	}

	return true
}

// recordsMatch checks if two records match, ignoring specified columns
func (p *CSVProcessor) recordsMatch(r1, r2 []string) bool {
	if len(r1) != len(r2) {
		return false
	}

	for i, v := range r1 {
		// Skip comparison for ignored columns
		if i < len(p.Headers) {
			colName := p.Headers[i]
			if p.ignoredColumnsMap[colName] {
				continue
			}
		}

		if v != r2[i] {
			return false
		}
	}

	return true
}

// CompareCSVsParallel compares two CSV files using parallel processing for better performance
func (p *CSVProcessor) CompareCSVsParallel(deltaCSVPath, changeLogPath string, chunkSize int) error {
	fileutils.LogInfo("Starting parallel comparison between files with chunk size: %s", fileutils.Highlight(fmt.Sprintf("%d", chunkSize)))

	// Load both files into memory
	prevData, err := loadCSVToMap(p.PreviousFile, p.PrimaryKey)
	if err != nil {
		fileutils.LogError("Error loading previous file: %v", err)
		return fmt.Errorf("error loading previous file: %w", err)
	}

	currData, headers, err := loadCSVToSlice(p.CurrentFile, p.PrimaryKey)
	if err != nil {
		fileutils.LogError("Error loading current file: %v", err)
		return fmt.Errorf("error loading current file: %w", err)
	}

	p.Headers = headers
	for i, header := range p.Headers {
		p.headerMap[header] = i
	}

	// Set current row count
	p.CurrentRowCount = len(currData)

	// Find primary key index
	keyIndex := p.headerMap[p.PrimaryKey]

	// Create delta CSV file
	deltaFile, err := os.Create(deltaCSVPath)
	if err != nil {
		return fmt.Errorf("error creating delta file: %w", err)
	}
	defer deltaFile.Close()

	deltaWriter := csv.NewWriter(deltaFile)
	defer deltaWriter.Flush()

	// Write headers to delta file
	if err := deltaWriter.Write(p.Headers); err != nil {
		return fmt.Errorf("error writing headers to delta file: %w", err)
	}

	// Create changes log file
	changesFile, err := os.Create(changeLogPath)
	if err != nil {
		return fmt.Errorf("error creating changes log file: %w", err)
	}
	defer changesFile.Close()

	// Create stats file
	statsPath := strings.TrimSuffix(deltaCSVPath, filepath.Ext(deltaCSVPath)) + "_stats.json"
	statsFile, err := os.Create(statsPath)
	if err != nil {
		return fmt.Errorf("error creating stats file: %w", err)
	}
	defer statsFile.Close()

	// Initialize stats
	stats := CompareStats{}

	// Process in parallel
	numWorkers := 4 // Adjust based on available CPU cores
	var wg sync.WaitGroup

	// Channel for results
	deltaRecordsCh := make(chan []string, numWorkers)
	changesCh := make(chan DeltaRecord, numWorkers)

	// Channel for distributing work
	workCh := make(chan [][]string, numWorkers)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for chunk := range workCh {
				for _, currRecord := range chunk {
					if len(currRecord) <= keyIndex {
						continue
					}

					key := currRecord[keyIndex]

					// Protect map access with mutex
					p.prevDataMutex.Lock()
					prevRecord, exists := prevData[key]

					// If record doesn't exist in previous file or has changes, write to delta
					if !exists || !p.recordsMatch(prevRecord, currRecord) {
						deltaRecordsCh <- currRecord
						p.DeltaRowCount++

						// If record doesn't exist in previous file, increment new records count
						if !exists {
							stats.NewRecords++
						}

						// If record exists in previous file, log changes
						if exists {
							deltaRecord := DeltaRecord{
								PrimaryKey: key,
								Changes:    make(map[string]ColumnChange),
							}

							for i, val := range currRecord {
								if i < len(prevRecord) && val != prevRecord[i] && i < len(p.Headers) {
									colName := p.Headers[i]

									// Skip ignored columns
									if p.ignoredColumnsMap[colName] {
										continue
									}

									deltaRecord.Changes[colName] = ColumnChange{
										Column:      colName,
										PreviousVal: prevRecord[i],
										CurrentVal:  val,
									}
									p.ChangedColumns[colName] = true
								}
							}

							if len(deltaRecord.Changes) > 0 {
								changesCh <- deltaRecord
							}
						}
					}

					// Remove processed record from prevData to identify records that exist only in previous file
					delete(prevData, key)
					p.prevDataMutex.Unlock()
				}
			}
		}()
	}

	// Distribute work
	go func() {
		for i := 0; i < len(currData); i += chunkSize {
			end := i + chunkSize
			if end > len(currData) {
				end = len(currData)
			}
			workCh <- currData[i:end]
		}
		close(workCh)
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(deltaRecordsCh)
		close(changesCh)
	}()

	// Write delta records
	var changes []DeltaRecord

	// Write delta records as they come in
	go func() {
		for record := range deltaRecordsCh {
			deltaWriter.Write(record)
		}
	}()

	// Collect changes
	for change := range changesCh {
		changes = append(changes, change)
	}

	// Flush delta writer
	deltaWriter.Flush()

	// Convert changes to map with primary keys as top-level properties
	changesMap := make(map[string]map[string]ColumnChange)
	for _, change := range changes {
		changesMap[change.PrimaryKey] = change.Changes
	}

	// Write changes to JSON file
	encoder := json.NewEncoder(changesFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(changesMap); err != nil {
		return fmt.Errorf("error writing changes to log file: %w", err)
	}

	// Calculate removed records (records that exist in prevData but not in current)
	stats.RemovedRecords = len(prevData)

	// Write stats to JSON file
	statsEncoder := json.NewEncoder(statsFile)
	statsEncoder.SetIndent("", "  ")
	if err := statsEncoder.Encode(stats); err != nil {
		return fmt.Errorf("error writing stats to file: %w", err)
	}

	// Generate Excel file with highlights
	excelPath := strings.TrimSuffix(deltaCSVPath, filepath.Ext(deltaCSVPath)) + ".xlsx"
	if err := p.generateExcelWithHighlights(deltaCSVPath, changesMap, excelPath); err != nil {
		return fmt.Errorf("error generating Excel file: %w", err)
	}

	// Print summary
	fmt.Println("\nSummary:")
	fmt.Printf("Current file row count: %d\n", p.CurrentRowCount)
	fmt.Printf("Delta file row count: %d\n", p.DeltaRowCount)
	fmt.Printf("New records: %d\n", stats.NewRecords)
	fmt.Printf("Removed records: %d\n", stats.RemovedRecords)
	fmt.Println("Columns with differences:")
	for col := range p.ChangedColumns {
		fmt.Printf("  - %s\n", col)
	}
	fmt.Printf("\nExcel file generated: %s\n", excelPath)
	fmt.Printf("Stats file generated: %s\n", statsPath)

	return nil
}

// openCSVReader opens a CSV file (supporting gzip if needed) and returns a CSV reader and a closer function
func openCSVReader(filePath string) (*csv.Reader, func(), error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}

	// Check if file is gzipped
	var reader io.Reader = file
	var closer = func() { file.Close() }

	if strings.HasSuffix(filePath, ".gz") {
		fileutils.LogInfo("Opening gzipped file: %s", fileutils.Highlight(filepath.Base(filePath)))
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, fmt.Errorf("error creating gzip reader: %w", err)
		}
		reader = gzReader
		closer = func() {
			gzReader.Close()
			file.Close()
		}
	} else {
		fileutils.LogInfo("Opening CSV file: %s", fileutils.Highlight(filepath.Base(filePath)))
	}

	return csv.NewReader(reader), closer, nil
}

// loadCSVToMap loads a CSV file into a map for fast lookups
func loadCSVToMap(filePath string, primaryKey string) (map[string][]string, error) {
	reader, closer, err := openCSVReader(filePath)
	if err != nil {
		return nil, err
	}
	defer closer()

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// Find primary key index
	keyIndex := -1
	for i, header := range headers {
		if header == primaryKey {
			keyIndex = i
			break
		}
	}
	if keyIndex == -1 {
		return nil, fmt.Errorf("primary key '%s' not found in headers", primaryKey)
	}

	data := make(map[string][]string)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(record) > keyIndex {
			data[record[keyIndex]] = record
		}
	}

	return data, nil
}

// loadCSVToSlice loads a CSV file into a slice for sequential processing
func loadCSVToSlice(filePath string, primaryKey string) ([][]string, []string, error) {
	reader, closer, err := openCSVReader(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer closer()

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, nil, err
	}

	// Verify primary key exists
	keyIndex := -1
	for i, header := range headers {
		if header == primaryKey {
			keyIndex = i
			break
		}
	}
	if keyIndex == -1 {
		return nil, nil, fmt.Errorf("primary key '%s' not found in headers", primaryKey)
	}

	var data [][]string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		data = append(data, record)
	}

	return data, headers, nil
}

// generateExcelWithHighlights creates an Excel file with highlighted changes and tooltips
func (p *CSVProcessor) generateExcelWithHighlights(deltaCSVPath string, changesMap map[string]map[string]ColumnChange, excelPath string) error {
	// Create a new Excel file
	f := excelize.NewFile()

	// Create a new sheet
	sheetName := "Changes"
	f.SetSheetName("Sheet1", sheetName)

	// Read the delta CSV file
	file, err := os.Open(deltaCSVPath)
	if err != nil {
		f.Close()
		return fmt.Errorf("error opening delta file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Make the reader more lenient with field counts
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	headers, err := reader.Read()
	if err != nil {
		f.Close()
		return fmt.Errorf("error reading headers: %w", err)
	}

	// Verify primary key exists in headers
	pkIndex := -1
	for i, header := range headers {
		if header == p.PrimaryKey {
			pkIndex = i
			break
		}
	}
	if pkIndex == -1 {
		f.Close()
		return fmt.Errorf("primary key '%s' not found in headers", p.PrimaryKey)
	}

	// Determine which columns to include
	columnsToInclude := make(map[int]bool)
	columnIndices := make([]int, 0)

	// Always include primary key column first
	columnsToInclude[pkIndex] = true
	columnIndices = append(columnIndices, pkIndex)

	// Include columns that had changes
	for col := range p.ChangedColumns {
		if index, exists := p.headerMap[col]; exists {
			columnsToInclude[index] = true
			columnIndices = append(columnIndices, index)
		}
	}

	// Sort column indices to maintain original order
	sort.Ints(columnIndices)

	// Write headers for included columns
	for i, colIndex := range columnIndices {
		colName, err := excelize.ColumnNumberToName(i + 1)
		if err != nil {
			f.Close()
			return fmt.Errorf("error converting column number to name: %w", err)
		}
		cell := colName + "1"
		if err := f.SetCellValue(sheetName, cell, headers[colIndex]); err != nil {
			f.Close()
			return fmt.Errorf("error writing header at cell %s: %w", cell, err)
		}
	}

	// Create style for highlighted cells
	style, err := f.NewStyle(&excelize.Style{
		Fill: excelize.Fill{
			Type:    "pattern",
			Color:   []string{"#FFEB9C"},
			Pattern: 1,
		},
	})
	if err != nil {
		f.Close()
		return fmt.Errorf("error creating style: %w", err)
	}

	// Process each row
	rowNum := 2  // Start from row 2 (after headers)
	lineNum := 2 // Track line number for error reporting
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return fmt.Errorf("error reading record at line %d: %w", lineNum, err)
		}

		// Get the primary key value
		if pkIndex >= len(record) {
			fmt.Printf("Warning: Skipping row %d: primary key column not found\n", lineNum)
			lineNum++
			continue
		}
		primaryKey := record[pkIndex]

		// Write the row (only included columns)
		for i, colIndex := range columnIndices {
			colName, err := excelize.ColumnNumberToName(i + 1)
			if err != nil {
				f.Close()
				return fmt.Errorf("error converting column number to name: %w", err)
			}
			cell := fmt.Sprintf("%s%d", colName, rowNum)

			// Handle case where record has fewer fields than expected
			if colIndex < len(record) {
				// Ensure the value is properly formatted as a string
				value := record[colIndex]
				if value == "" {
					value = " " // Use a space instead of empty string to prevent Excel from deleting the column
				}
				// Clean the value to prevent Excel corruption
				value = cleanExcelValue(value)
				if err := f.SetCellValue(sheetName, cell, value); err != nil {
					f.Close()
					return fmt.Errorf("error writing value at cell %s: %w", cell, err)
				}
			} else {
				if err := f.SetCellValue(sheetName, cell, " "); err != nil {
					f.Close()
					return fmt.Errorf("error writing empty value at cell %s: %w", cell, err)
				}
			}

			// If this row has changes, highlight the changed cells
			if changes, exists := changesMap[primaryKey]; exists {
				colName := headers[colIndex]
				if change, hasChange := changes[colName]; hasChange {
					// Apply highlight style
					if err := f.SetCellStyle(sheetName, cell, cell, style); err != nil {
						f.Close()
						return fmt.Errorf("error applying style at cell %s: %w", cell, err)
					}

					// Add comment with previous value
					comment := fmt.Sprintf("Previous value: %v", cleanExcelValue(fmt.Sprintf("%v", change.PreviousVal)))
					if err := f.AddComment(sheetName, excelize.Comment{
						Cell:   cell,
						Author: "CSV Compare",
						Text:   comment,
					}); err != nil {
						f.Close()
						return fmt.Errorf("error adding comment at cell %s: %w", cell, err)
					}
				}
			}
		}
		rowNum++
		lineNum++
	}

	// Auto-fit columns
	for i := range columnIndices {
		colName, err := excelize.ColumnNumberToName(i + 1)
		if err != nil {
			f.Close()
			return fmt.Errorf("error converting column number to name: %w", err)
		}
		if err := f.SetColWidth(sheetName, colName, colName, 15); err != nil {
			f.Close()
			return fmt.Errorf("error setting column width for %s: %w", colName, err)
		}
	}

	// Save the file
	if err := f.SaveAs(excelPath); err != nil {
		f.Close()
		return fmt.Errorf("error saving Excel file: %w", err)
	}

	// Close the file
	if err := f.Close(); err != nil {
		return fmt.Errorf("error closing Excel file: %w", err)
	}

	return nil
}

// cleanExcelValue sanitizes a value to prevent Excel corruption
func cleanExcelValue(value string) string {
	// Remove any control characters
	value = strings.Map(func(r rune) rune {
		if r < 32 && r != '\t' && r != '\n' && r != '\r' {
			return -1
		}
		return r
	}, value)

	// Replace any problematic characters
	value = strings.ReplaceAll(value, "\x00", "")
	value = strings.ReplaceAll(value, "\x1A", "")
	value = strings.ReplaceAll(value, "\x1B", "")
	value = strings.ReplaceAll(value, "\x1C", "")
	value = strings.ReplaceAll(value, "\x1D", "")
	value = strings.ReplaceAll(value, "\x1E", "")
	value = strings.ReplaceAll(value, "\x1F", "")

	// Ensure the value is not too long (Excel has a limit of 32,767 characters per cell)
	if len(value) > 32000 {
		value = value[:32000]
	}

	return value
}
