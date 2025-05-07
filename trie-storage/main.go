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
	EnableHistory   bool
	SkipFiles       int
	Metadata        []Metadata
	Analysis        *Analysis
}

func main() {
	config := parseFlags()
	if err := loadConfiguration(config); err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	processor := NewDataProcessor(config)
	if err := processor.ProcessAssetFiles(config.Analysis.AssetFiles); err != nil {
		log.Fatalf("Error processing files: %v", err)
	}
}

func loadConfiguration(config *Config) error {
	// Load metadata
	log.Printf("Loading metadata from %s", config.MetadataFile)
	metadata, err := loadMetadata(config.MetadataFile)
	if err != nil {
		return err
	}
	config.Metadata = metadata
	log.Printf("Successfully loaded metadata with %d entries", len(metadata))

	// Load analysis
	log.Printf("Loading analysis from %s", config.AnalysisFile)
	analysis, err := loadAnalysis(config.AnalysisFile)
	if err != nil {
		return err
	}
	config.Analysis = analysis
	log.Printf("Successfully loaded analysis")

	return nil
}

func parseFlags() *Config {
	sourceRoot := flag.String("source", "", "Source data root path")
	assetDestRoot := flag.String("asset-dest", "", "Destination root path for asset files")
	companyDestRoot := flag.String("company-dest", "", "Destination root path for company files")
	metadataFile := flag.String("metadata", "metadata.json", "Path to metadata.json file")
	analysisFile := flag.String("analysis", "analysis.json", "Path to analysis.json file")
	enableHistory := flag.Bool("history", false, "Enable history tracking")
	skipFiles := flag.Int("skip", 0, "Number of files to skip (for resuming processing)")
	flag.Parse()

	if *sourceRoot == "" || *assetDestRoot == "" || *companyDestRoot == "" {
		log.Fatal("Source root, asset destination root, and company destination root must be specified")
	}

	return &Config{
		SourceRoot:      *sourceRoot,
		AssetDestRoot:   *assetDestRoot,
		CompanyDestRoot: *companyDestRoot,
		MetadataFile:    *metadataFile,
		AnalysisFile:    *analysisFile,
		EnableHistory:   *enableHistory,
		SkipFiles:       *skipFiles,
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