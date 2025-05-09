# CSV Change Visualizer

A tool to visualize changes in CSV files by highlighting modified cells and showing previous values.

## Requirements

```
flask==2.0.1
pandas==1.3.3
numpy==1.21.2
openpyxl==3.0.9
```

## Installation

Install the required packages:

```bash
pip install flask pandas numpy openpyxl
```

## Usage

1. Run the application:
   ```bash
   python app.py
   ```

2. Open your browser and navigate to `http://localhost:5000`

3. Enter the paths to your CSV file and changes JSON file (or use the defaults)

4. Click "Process Changes" to generate an Excel file with highlighted changes

5. Download the Excel file to view the changes
