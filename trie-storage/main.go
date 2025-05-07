package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
)

type Config struct {
	SourceRoot      string
	AssetDestRoot   string
	CompanyDestRoot string
}

func main() {
	config := parseFlags()
	
	// Load metadata
	metadata, err := loadMetadata()
	if err != nil {
		log.Fatalf("Failed to load metadata: %v", err)
	}

	// Load analysis
	analysis, err := loadAnalysis()
	if err != nil {
		log.Fatalf("Failed to load analysis: %v", err)
	}

	// Process asset files
	processor := NewDataProcessor(config, metadata)
	if err := processor.ProcessAssetFiles(analysis.AssetFiles); err != nil {
		log.Fatalf("Failed to process asset files: %v", err)
	}
}

func parseFlags() Config {
	sourceRoot := flag.String("source", "", "Source data root path")
	assetDestRoot := flag.String("asset-dest", "", "Destination root path for asset files")
	companyDestRoot := flag.String("company-dest", "", "Destination root path for company files")
	flag.Parse()

	if *sourceRoot == "" || *assetDestRoot == "" || *companyDestRoot == "" {
		log.Fatal("Source root, asset destination root, and company destination root must be specified")
	}

	return Config{
		SourceRoot:      *sourceRoot,
		AssetDestRoot:   *assetDestRoot,
		CompanyDestRoot: *companyDestRoot,
	}
}

func loadMetadata() ([]Metadata, error) {
	data, err := os.ReadFile("metadata.json")
	if err != nil {
		return nil, err
	}

	var metadata []Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return metadata, nil
}

func loadAnalysis() (*Analysis, error) {
	data, err := os.ReadFile("analysis.json")
	if err != nil {
		return nil, err
	}

	var analysis Analysis
	if err := json.Unmarshal(data, &analysis); err != nil {
		return nil, err
	}

	return &analysis, nil
} 