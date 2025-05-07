package main

type Analysis struct {
	AssetFiles   []string `json:"assetFiles"`
	CompanyFiles []string `json:"companyFiles"`
	Exceptions   []string `json:"exceptions"`
}

type Metadata struct {
	Filename     string     `json:"filename"`
	RelativePath string     `json:"relativePath"`
	EffectiveDate string    `json:"effectiveDate"`
	RowCount     int        `json:"rowCount"`
	Columns      []Column   `json:"columns"`
}

type Column struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
}

type PropertyHistory struct {
	PropertyName string    `json:"propertyName"`
	History      []History `json:"history"`
}

type History struct {
	Value        interface{} `json:"value"`
	SourceFile   string      `json:"sourceFile"`
	EffectiveDate string     `json:"effectiveDate"`
}

type AssetData map[string]interface{} 