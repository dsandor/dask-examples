package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"csv-compare/pkg/csvprocessor"
	"csv-compare/pkg/fileutils"

	"github.com/fatih/color"
)

// printBanner prints a colorful banner for the application
func printBanner() {
	bold := color.New(color.Bold).SprintFunc()
	cyan := color.New(color.FgCyan, color.Bold).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	
	fmt.Println("")
	fmt.Println(cyan("  ____   ______     __   ____                                  "))
	fmt.Println(cyan(" / ___\\ /_  __/    / /  / __ \\___  __ _  ___  __ _ _ __ ___ "))
	fmt.Println(cyan("/ /     / /      / /  / /  / _ \\/ _` |/ _ \\/ _` | '__/ _ \\"))
	fmt.Println(cyan("\\/___  / /      / /__/ /__/  __/ (_| |  __/ (_| | | |  __/"))
	fmt.Println(cyan(" \\____\\/_/      /____/\\____/\\___|\\__, |\\___|\\__,_|_|  \\___|"))
	fmt.Println(cyan("                                 |___/                     "))
	fmt.Println("")
	fmt.Printf("%s %s\n", bold("CSV Compare Tool"), green("v1.1.0"))
	fmt.Println(green("Efficiently compare CSV files and generate delta reports"))
	fmt.Println("")
}

func main() {
	// Print banner
	printBanner()
	
	// Define command-line flags
	dirPath := flag.String("dir", ".", "Directory path containing CSV files (.csv or .csv.gz)")
	deltaPath := flag.String("delta", "", "Path for the delta CSV output (default: delta_TIMESTAMP.csv)")
	logPath := flag.String("log", "", "Path for the changes log JSON output (default: changes_TIMESTAMP.json)")
	useParallel := flag.Bool("parallel", true, "Use parallel processing for better performance")
	chunkSize := flag.Int("chunk-size", 1000, "Chunk size for parallel processing")
	cpuProfile := flag.String("cpuprofile", "", "Write CPU profile to file")
	memProfile := flag.String("memprofile", "", "Write memory profile to file")
	
	// Custom CSV files
	prevFile := flag.String("prev", "", "Specific previous CSV file to use (instead of auto-detecting)")
	currFile := flag.String("curr", "", "Specific current CSV file to use (instead of auto-detecting)")
	
	// Columns to ignore when comparing
	ignoreColumnsStr := flag.String("ignore-columns", "", "Comma-separated list of column names to ignore when determining differences")

	flag.Parse()

	// CPU profiling if requested
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Set default output paths if not provided
	timestamp := time.Now().Format("20060102_150405")
	if *deltaPath == "" {
		*deltaPath = fmt.Sprintf("delta_%s.csv", timestamp)
	}
	if *logPath == "" {
		*logPath = fmt.Sprintf("changes_%s.json", timestamp)
	}
	
	fileutils.LogInfo("Starting CSV Compare tool")

	// Find CSV files
	var previousFile, currentFile string
	var err error

	if *prevFile != "" && *currFile != "" {
		// Use specified files
		previousFile = *prevFile
		currentFile = *currFile
	} else {
		// Auto-detect latest files
		currentFile, previousFile, err = fileutils.FindLatestCSVFiles(*dirPath)
		if err != nil {
			log.Fatalf("Error finding CSV files: %v", err)
		}
	}

	fmt.Println("")
	fileutils.LogInfo("Comparing files:")
	prevType := "CSV"
	if strings.HasSuffix(previousFile, ".gz") {
		prevType = "Gzipped CSV"
	}
	currType := "CSV"
	if strings.HasSuffix(currentFile, ".gz") {
		currType = "Gzipped CSV"
	}
	
	fmt.Printf("  Previous: %s (%s)\n", fileutils.Highlight(filepath.Base(previousFile)), prevType)
	fmt.Printf("  Current:  %s (%s)\n", fileutils.Highlight(filepath.Base(currentFile)), currType)
	fmt.Println("")
	fileutils.LogInfo("Output files:")
	fmt.Printf("  Delta:    %s\n", fileutils.Highlight(*deltaPath))
	fmt.Printf("  Changes:  %s\n", fileutils.Highlight(*logPath))

	// Parse ignored columns
	var ignoredColumns []string
	if *ignoreColumnsStr != "" {
		ignoredColumns = strings.Split(*ignoreColumnsStr, ",")
		// Trim whitespace from column names
		for i := range ignoredColumns {
			ignoredColumns[i] = strings.TrimSpace(ignoredColumns[i])
		}
	}

	// Create processor
	processor := csvprocessor.NewCSVProcessor(previousFile, currentFile, ignoredColumns)

	// Start timer
	startTime := time.Now()

	// Process files
	if *useParallel {
		fileutils.LogInfo("Using %s", fileutils.Highlight("parallel processing"))
		err = processor.CompareCSVsParallel(*deltaPath, *logPath, *chunkSize)
	} else {
		fileutils.LogInfo("Using %s", fileutils.Highlight("sequential processing"))
		err = processor.CompareCSVs(*deltaPath, *logPath)
	}

	if err != nil {
		log.Fatalf("Error processing CSV files: %v", err)
	}

	// Print execution time
	duration := time.Since(startTime)
	fileutils.LogSuccess("Processing completed in %s", fileutils.Highlight(duration.String()))

	// Memory profiling if requested
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
