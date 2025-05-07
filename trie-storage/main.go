package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
)

type Config struct {
	SourceRoot      string
	AssetDestRoot   string
	CompanyDestRoot string
	MetadataFile    string
	AnalysisFile    string
}

func main() {
	logger := NewLogger()
	config := parseFlags()
	
	logger.Info("Starting application with configuration:")
	logger.Info("Source Root: %s", logger.HighlightFile(config.SourceRoot))
	logger.Info("Asset Destination Root: %s", logger.HighlightFile(config.AssetDestRoot))
	logger.Info("Company Destination Root: %s", logger.HighlightFile(config.CompanyDestRoot))
	logger.Info("Metadata File: %s", logger.HighlightFile(config.MetadataFile))
	logger.Info("Analysis File: %s", logger.HighlightFile(config.AnalysisFile))
	
	// Load metadata
	logger.Info("Loading metadata from %s", logger.HighlightFile(config.MetadataFile))
	metadata, err := loadMetadata(config.MetadataFile)
	if err != nil {
		logger.Error("Failed to load metadata: %v", err)
		os.Exit(1)
	}
	logger.Success("Successfully loaded metadata with %d entries", len(metadata))

	// Load analysis
	logger.Info("Loading analysis from %s", logger.HighlightFile(config.AnalysisFile))
	analysis, err := loadAnalysis(config.AnalysisFile)
	if err != nil {
		logger.Error("Failed to load analysis: %v", err)
		os.Exit(1)
	}
	logger.Success("Successfully loaded analysis")

	// Process asset files
	logger.Info("Processing %d asset files", len(analysis.AssetFiles))
	processor := NewDataProcessor(config, metadata, logger)
	if err := processor.ProcessAssetFiles(analysis.AssetFiles); err != nil {
		logger.Error("Failed to process asset files: %v", err)
		os.Exit(1)
	}
	logger.Success("Successfully processed all asset files")
}

func parseFlags() Config {
	sourceRoot := flag.String("source", "", "Source data root path")
	assetDestRoot := flag.String("asset-dest", "", "Destination root path for asset files")
	companyDestRoot := flag.String("company-dest", "", "Destination root path for company files")
	metadataFile := flag.String("metadata", "metadata.json", "Path to metadata.json file")
	analysisFile := flag.String("analysis", "analysis.json", "Path to analysis.json file")
	flag.Parse()

	if *sourceRoot == "" || *assetDestRoot == "" || *companyDestRoot == "" {
		log.Fatal("Source root, asset destination root, and company destination root must be specified")
	}

	return Config{
		SourceRoot:      *sourceRoot,
		AssetDestRoot:   *assetDestRoot,
		CompanyDestRoot: *companyDestRoot,
		MetadataFile:    *metadataFile,
		AnalysisFile:    *analysisFile,
	}
}

func loadMetadata(filePath string) ([]Metadata, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var metadata []Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return metadata, nil
}

func loadAnalysis(filePath string) (*Analysis, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var analysis Analysis
	if err := json.Unmarshal(data, &analysis); err != nil {
		return nil, err
	}

	return &analysis, nil
} 