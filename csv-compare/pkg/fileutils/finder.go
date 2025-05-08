package fileutils

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
)

// FileInfo holds information about a file
type FileInfo struct {
	Path     string
	ModTime  time.Time
	Date     time.Time
	DateStr  string
	Filename string
}

// Color setup for logging
var (
	infoColor    = color.New(color.FgCyan).SprintFunc()
	successColor = color.New(color.FgGreen, color.Bold).SprintFunc()
	errorColor   = color.New(color.FgRed, color.Bold).SprintFunc()
	warnColor    = color.New(color.FgYellow).SprintFunc()
	highlightColor = color.New(color.FgMagenta, color.Bold).SprintFunc()
)

// LogInfo prints an info message with color
func LogInfo(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", infoColor("[INFO]:"), fmt.Sprintf(format, args...))
}

// LogSuccess prints a success message with color
func LogSuccess(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", successColor("[SUCCESS]:"), fmt.Sprintf(format, args...))
}

// LogError prints an error message with color
func LogError(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", errorColor("[ERROR]:"), fmt.Sprintf(format, args...))
}

// LogWarn prints a warning message with color
func LogWarn(format string, args ...interface{}) {
	fmt.Printf("%s %s\n", warnColor("[WARNING]:"), fmt.Sprintf(format, args...))
}

// Highlight returns a highlighted string
func Highlight(s string) string {
	return highlightColor(s)
}

// FindLatestCSVFiles finds the latest and previous CSV files in a directory
// Now supports .csv.gz files and uses date in filename (YYYYMMDD format)
func FindLatestCSVFiles(dirPath string) (latest, previous string, err error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		LogError("Failed to read directory: %v", err)
		return "", "", err
	}

	// Regular expression to extract date in YYYYMMDD format from filename
	dateRegex := regexp.MustCompile(`(\d{8})`)
	
	var csvFiles []FileInfo

	LogInfo("Scanning directory: %s", dirPath)
	
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		// Check for both .csv and .csv.gz extensions
		if strings.HasSuffix(filename, ".csv") || strings.HasSuffix(filename, ".csv.gz") {
			info, err := file.Info()
			if err != nil {
				LogWarn("Could not get file info for %s: %v", filename, err)
				continue
			}

			// Extract date from filename
			dateMatch := dateRegex.FindStringSubmatch(filename)
			var fileDate time.Time
			var dateStr string
			
			if len(dateMatch) > 1 {
				dateStr = dateMatch[1]
				year, _ := strconv.Atoi(dateStr[0:4])
				month, _ := strconv.Atoi(dateStr[4:6])
				day, _ := strconv.Atoi(dateStr[6:8])
				fileDate = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
			} else {
				// If no date in filename, use modification time
				fileDate = info.ModTime()
				dateStr = "unknown"
			}

			csvFiles = append(csvFiles, FileInfo{
				Path:     filepath.Join(dirPath, filename),
				ModTime:  info.ModTime(),
				Date:     fileDate,
				DateStr:  dateStr,
				Filename: filename,
			})
			
			dateDisplay := warnColor(dateStr)
			if dateStr != "unknown" {
				dateDisplay = Highlight(dateStr)
			}
			LogInfo("Found CSV file: %s with date: %s", Highlight(filename), dateDisplay)
		}
	}

	if len(csvFiles) < 2 {
		LogError("Not enough CSV files found (need at least 2)")
		return "", "", os.ErrNotExist
	}

	// Sort files by date (extracted from filename) instead of modification time
	sort.Slice(csvFiles, func(i, j int) bool {
		// If dates are the same, use modification time as a tiebreaker
		if csvFiles[i].Date.Equal(csvFiles[j].Date) {
			return csvFiles[i].ModTime.After(csvFiles[j].ModTime)
		}
		return csvFiles[i].Date.After(csvFiles[j].Date)
	})

	latest = csvFiles[0].Path
	previous = csvFiles[1].Path
	
	latestDateDisplay := warnColor(csvFiles[0].DateStr)
	if csvFiles[0].DateStr != "unknown" {
		latestDateDisplay = Highlight(csvFiles[0].DateStr)
	}
	LogSuccess("Latest file: %s (Date: %s)", Highlight(csvFiles[0].Filename), latestDateDisplay)
	prevDateDisplay := warnColor(csvFiles[1].DateStr)
	if csvFiles[1].DateStr != "unknown" {
		prevDateDisplay = Highlight(csvFiles[1].DateStr)
	}
	LogSuccess("Previous file: %s (Date: %s)", Highlight(csvFiles[1].Filename), prevDateDisplay)

	return latest, previous, nil
}
