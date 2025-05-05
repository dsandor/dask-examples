package main

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	// Create test directory structure
	testDirs := []struct {
		dirName  string
		fileName string
		content  string
	}{
		{
			dirName:  "customers",
			fileName: "customers_20240101.gz",
			content:  "id,name,email,age,active,created_at,ID_BB_COMPANY\n1,John Doe,john@example.com,30,true,2023-01-15 10:30:45,COMP123\n2,Jane Smith,jane@example.com,25,true,2023-02-20 14:22:33,COMP456\n3,Bob Johnson,bob@example.com,40,false,2023-03-10 09:15:27,COMP789\n",
		},
		{
			dirName:  "products",
			fileName: "products_20240215.gz",
			content:  "product_id,name,price,in_stock,category,last_updated,ID_BB_GLOBAL\n101,Laptop,1299.99,true,Electronics,2024-01-05,BBG0001\n102,Desk Chair,199.50,true,Furniture,2024-01-10,BBG0002\n103,Coffee Maker,89.95,false,Appliances,2024-01-15,BBG0003\n104,Headphones,149.99,true,Electronics,2024-01-20,BBG0004\n",
		},
		{
			dirName:  "orders",
			fileName: "orders_20240320.gz",
			content:  "order_id,customer_id,total_amount,order_date,status\n1001,1,1299.99,2024-02-01,Shipped\n1002,2,199.50,2024-02-05,Delivered\n1003,3,239.94,2024-02-10,Processing\n1004,1,89.95,2024-02-15,Pending\n1005,2,149.99,2024-02-20,Shipped\n",
		},
		{
			dirName:  "securities",
			fileName: "securities_20240410.gz",
			content:  "security_id,name,ticker,price,sector,ID_BB_GLOBAL\n201,Apple Inc.,AAPL,175.25,Technology,BBG000B9XRY4\n202,Microsoft Corp.,MSFT,325.75,Technology,BBG000BPH459\n203,Amazon.com Inc.,AMZN,130.50,Consumer Cyclical,BBG000BVPV84\n204,Tesla Inc.,TSLA,190.20,Automotive,BBG000N9MNX3\n",
		},
		{
			dirName:  "companies",
			fileName: "companies_20240501.gz",
			content:  "company_id,name,industry,country,employees,ID_BB_COMPANY\n301,Acme Corp,Manufacturing,USA,5000,BBG001S5PQL8\n302,TechSolutions Inc,Technology,USA,2500,BBG000C7P572\n303,Global Logistics Ltd,Transportation,UK,3500,BBG000BSJK37\n304,EcoEnergy Group,Energy,Germany,1200,BBG000BS1YK4\n",
		},
	}

	// Create base directory
	baseDir := "testdata"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		fmt.Printf("Error creating base directory: %v\n", err)
		return
	}

	// Create test files
	for _, td := range testDirs {
		// Create directory
		dirPath := filepath.Join(baseDir, td.dirName)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			fmt.Printf("Error creating directory %s: %v\n", dirPath, err)
			continue
		}

		// Create gzipped file
		filePath := filepath.Join(dirPath, td.fileName)
		file, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("Error creating file %s: %v\n", filePath, err)
			continue
		}

		// Create gzip writer
		gzWriter := gzip.NewWriter(file)
		_, err = gzWriter.Write([]byte(td.content))
		if err != nil {
			fmt.Printf("Error writing to gzip file %s: %v\n", filePath, err)
			file.Close()
			continue
		}

		// Close writers
		if err := gzWriter.Close(); err != nil {
			fmt.Printf("Error closing gzip writer for %s: %v\n", filePath, err)
		}
		if err := file.Close(); err != nil {
			fmt.Printf("Error closing file %s: %v\n", filePath, err)
		}

		fmt.Printf("Created test file: %s\n", filePath)
	}

	fmt.Println("Test data creation complete")
}
