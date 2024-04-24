
# Data Pipeline for Cloud Provider Exports

This Python script processes export files from cloud providers, such as Azure, converting them to the FOCUS format for further analysis. It handles data from input CSV files, converts them into Parquet format, processes these files to extract datasets month by month, and prepares them for data science purposes.

## Prerequisites

- Python 3.9 or later
- Access to cloud provider export files placed in the `input` directory

## Installation

Before running the script, ensure that all required Python packages are installed. These dependencies are listed in `requirements.txt`. Install them using the following command:

```bash
pip install -r requirements.txt
```

Additionally, this script uses the `focus-converter` module, specifically designed for FOCUS data conversion. More information about the FOCUS converter can be found on the [official GitHub repository](https://github.com/finopsfoundation/focus_converters/).

## Usage

1. **Prepare Your Data:**
   Place your cloud provider export files (e.g., from Azure) in the `input` folder. These files should be in CSV format.

2. **Run the Script:**
   Execute the script from your command line:

   ```bash
   python data_pipeline.py
   ```

3. **Output:**
   The script will process the input files, convert them to Parquet and then to the desired monthly datasets in CSV format stored in the `output` folder. Logs of the process will be displayed in the console, giving detailed information about the script's operations and any issues encountered.

## Logging Configuration

The script includes detailed logging that helps in tracing the processing steps and diagnosing issues with the data conversion process. Logs are displayed in the console with information such as timestamps, module names, severity levels, and messages.

## Folder Structure

- `input/`: Place your CSV files here.
- `output/`: Contains converted Parquet files and final CSV datasets.
- `output/parquet/`: Intermediate folder for Parquet files.
- `output/dataset/`: Contains the final datasets ready for data science analysis.

## Cleaning Up

After processing, the script cleans the intermediate Parquet files to free up space and avoid clutter.

## Contributing

Feel free to contribute to this project by forking the repository and submitting a pull request. For major changes, please open an issue first to discuss what you would like to change.

---

For more information and updates, check the project's main repository linked above.