package csvprocessor

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"csv-compare/pkg/fileutils"
)

// DeltaRecord represents a change between two CSV records
type DeltaRecord struct {
	RowIndex int
	Changes  map[string]ColumnChange
}

// ColumnChange represents a change in a column value
type ColumnChange struct {
	Column      string      `json:"column"`
	PreviousVal interface{} `json:"previous_value"`
	CurrentVal  interface{} `json:"current_value"`
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
	PrimaryKey        string // Add primary key field
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
					RowIndex: rowIndex,
					Changes:  make(map[string]ColumnChange),
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
				for rowIndex, currRecord := range chunk {
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

						// If record exists in previous file, log changes
						if exists {
							deltaRecord := DeltaRecord{
								RowIndex: rowIndex,
								Changes:  make(map[string]ColumnChange),
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

	// Write changes to JSON file
	encoder := json.NewEncoder(changesFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(changes); err != nil {
		return fmt.Errorf("error writing changes to log file: %w", err)
	}

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
