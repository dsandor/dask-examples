package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dsandor/meta-extract/pkg/analyzer"
	"github.com/dsandor/meta-extract/pkg/extractor"
)

func main() {
	// Define command flags
	extractCmd := flag.NewFlagSet("extract", flag.ExitOnError)
	analyzeCmd := flag.NewFlagSet("analyze", flag.ExitOnError)

	// Extract command flags
	extractRootPath := extractCmd.String("path", ".", "Root path to scan for files")
	extractOutputFile := extractCmd.String("output", "metadata.json", "Output file for metadata")

	// Analyze command flags
	analyzeInputFile := analyzeCmd.String("input", "metadata.json", "Input metadata JSON file to analyze")
	analyzeOutputFile := analyzeCmd.String("output", "analysis.json", "Output file for analysis results")

	// Check if a subcommand is provided
	if len(os.Args) < 2 {
		fmt.Println("Expected 'extract' or 'analyze' subcommand")
		fmt.Println("Usage:")
		fmt.Println("  meta-extract extract -path <root_path> -output <output_file>")
		fmt.Println("  meta-extract analyze -input <metadata_file> -output <analysis_file>")
		os.Exit(1)
	}

	// Parse the appropriate command
	switch os.Args[1] {
	case "extract":
		extractCmd.Parse(os.Args[2:])
		runExtract(*extractRootPath, *extractOutputFile)
	case "analyze":
		analyzeCmd.Parse(os.Args[2:])
		runAnalyze(*analyzeInputFile, *analyzeOutputFile)
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		fmt.Println("Expected 'extract' or 'analyze' subcommand")
		os.Exit(1)
	}
}

// runExtract runs the extract command to extract metadata from files
func runExtract(rootPath, outputFile string) {
	// Validate root path
	rootPathAbs, err := filepath.Abs(rootPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving path: %v\n", err)
		os.Exit(1)
	}

	// Check if path exists
	if _, err := os.Stat(rootPathAbs); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Path does not exist: %s\n", rootPathAbs)
		os.Exit(1)
	}

	fmt.Printf("Scanning directory: %s\n", rootPathAbs)

	// Extract metadata
	metadata, err := extractor.ExtractMetadata(rootPathAbs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error extracting metadata: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d files with metadata\n", len(metadata))

	// Write metadata to file
	outputPath := filepath.Join(rootPathAbs, outputFile)
	jsonData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	err = os.WriteFile(outputPath, jsonData, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing metadata file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Metadata written to: %s\n", outputPath)
}

// runAnalyze runs the analyze command to analyze metadata JSON
func runAnalyze(inputFile, outputFile string) {
	// Validate input file path
	inputPathAbs, err := filepath.Abs(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving input path: %v\n", err)
		os.Exit(1)
	}

	// Check if input file exists
	if _, err := os.Stat(inputPathAbs); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Input file does not exist: %s\n", inputPathAbs)
		os.Exit(1)
	}

	fmt.Printf("Analyzing metadata file: %s\n", inputPathAbs)

	// Analyze metadata
	result, err := analyzer.AnalyzeMetadata(inputPathAbs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error analyzing metadata: %v\n", err)
		os.Exit(1)
	}

	// Output summary
	fmt.Printf("Analysis complete:\n")
	fmt.Printf("- Asset files (with ID_BB_GLOBAL): %d\n", len(result.AssetFiles))
	fmt.Printf("- Company files (with ID_BB_COMPANY but no ID_BB_GLOBAL): %d\n", len(result.CompanyFiles))
	fmt.Printf("- Exception files (missing both ID columns): %d\n", len(result.Exceptions))

	// Log exceptions
	if len(result.Exceptions) > 0 {
		fmt.Println("\nExceptions (files missing both ID_BB_GLOBAL and ID_BB_COMPANY):")
		for _, exception := range result.Exceptions {
			fmt.Printf("- %s\n", exception)
		}
	}

	// Write analysis to file
	outputPathAbs, err := filepath.Abs(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving output path: %v\n", err)
		os.Exit(1)
	}

	err = analyzer.WriteAnalysisToFile(result, outputPathAbs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing analysis file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nAnalysis written to: %s\n", outputPathAbs)
}
