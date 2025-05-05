package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dsandor/meta-extract/pkg/extractor"
)

func main() {
	// Parse command line arguments
	rootPath := flag.String("path", ".", "Root path to scan for files")
	outputFile := flag.String("output", "metadata.json", "Output file for metadata")
	flag.Parse()

	// Validate root path
	rootPathAbs, err := filepath.Abs(*rootPath)
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
	outputPath := filepath.Join(rootPathAbs, *outputFile)
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
