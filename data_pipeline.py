import os
import polars as pl
import glob
import logging

# path to the input and output folder
input_folder = "input"
output_folder = "output"
parquet_folder = os.path.join(output_folder, "parquet")
dataset_folder = os.path.join(output_folder, "dataset")

# Check if focus-converter is installed
try:
    from focus_converter.main import FocusConverter, ProviderSensor, Validator
except ImportError as e:
    raise ImportError("focus-converter is not installed. Please install it using 'pip install focus-convert'") from e


def setup_logger():
    # Create a custom logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Define the level of the logger to DEBUG

    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Define the level of the handler to INFO

    # Define the format of the logs
    formatter = logging.Formatter(
        '[%(asctime)s][%(name)s][%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()


def convert_csv_to_focus(input_folder, output_folder):
    """
    Convert all CSV files in input_folder to FOCUS format and save them in output_folder
    :param input_folder: Source folder containing CSV files
    :param output_folder: Destination folder to save FOCUS files
    :return: None
    """
    logger.info(f"Converting CSV files in {input_folder} to FOCUS format and saving them in {output_folder}")
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    logger.info(f"Found {len(csv_files)} CSV files to convert")
    for file_path in csv_files:
        try:
            provider_sensor = ProviderSensor(base_path=file_path)
            provider_sensor.load()

            converter = FocusConverter()
            converter.load_provider_conversion_configs()
            converter.load_data(
                data_path=file_path,
                data_format=provider_sensor.data_format,
                parquet_data_format=provider_sensor.parquet_data_format,
            )
            converter.configure_data_export(
                export_path=output_folder,
                export_include_source_columns=True,
                basename_template=None,
            )
            converter.prepare_horizontal_conversion_plan(provider=provider_sensor.provider)
            converter.convert()
            logger.info(f"File {file_path} converted successfully")

            # Optional: Validate the output - Not working at the moment
            """ 
            for segment_file_name in os.listdir(output_folder):
                segment_path = os.path.join(output_folder, segment_file_name)
                validator = Validator(
                    data_filename=segment_path,
                    output_type="console",  # or another appropriate type based on your needs
                    output_destination=None  # or specify a file or other destination
                )
                validator.load()
                validator.validate()
                break  # Break after validating one file for demonstration
                
            """

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            continue


def process_parquet_files(parquet_folder, output_folder):
    logger.info(f"Processing parquet files in {parquet_folder} and saving them in {output_folder}")
    # List all parquet files
    parquet_files = glob.glob(os.path.join(parquet_folder, '*.parquet'))

    # Create an empty DataFrame
    full_df = pl.DataFrame()

    # Read file and concatenate them in a single DataFrame
    logger.info(f"Found {len(parquet_files)} parquet files to process")
    for file_path in parquet_files:
        df = pl.read_parquet(file_path)
        full_df = pl.concat([full_df, df])
        logger.info(f"File {file_path} read successfully")

    # Convert Date to datetime - Date format is : 2021-01-01 00:00:00
    full_df = full_df.with_columns(
        [
            # To datetime, the format of data is 'MM/DD/YYYY'
            pl.col('Date').str.to_date('%m/%d/%Y').alias('Date')
        ]
    )
    logger.info("Saving data to CSV files")
    # Save month by month. name of the file should be output_{month}-{year}.csv
    for month in full_df['Date'].dt.month().unique():
        month_df = full_df.filter(full_df['Date'].dt.month() == month)
        month_year = month_df['Date'].dt.strftime('%m-%Y')[0]
        month_df.write_csv(os.path.join(output_folder, f'output_{month_year}.csv'))
        logger.info(f"File output_{month_year}.csv saved successfully")


def process_for_datascience(dataset_folder, output_folder=None):
    """
    Process the dataset for datascience
    :param dataset_folder: Folder containing the dataset
    :return: None
    """
    logger.info(f"Processing dataset in {dataset_folder} for datascience")
    pass


def clean_folder(folder):
    """
    Clean a folder by removing all files
    :param folder:  Folder to clean
    :return: None
    """
    logger.info(f"Cleaning folder {folder}")
    files = glob.glob(os.path.join(folder, '*'))
    for file in files:
        os.remove(file)

    logger.info(f"Folder {folder} cleaned successfully")


def main():
    logger.info("====================================")
    logger.info("Starting data pipeline")
    logger.info("Last update: 2024-04-24")
    logger.info("Development phase: WIP")
    logger.info("====================================")
    # Create all folder if they don't exist
    os.makedirs(input_folder, exist_ok=True)
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(parquet_folder, exist_ok=True)
    os.makedirs(dataset_folder, exist_ok=True)

    # Convert CSV to FOCUS
    convert_csv_to_focus(input_folder, parquet_folder)

    # Process parquet files
    process_parquet_files(parquet_folder, dataset_folder)

    # Clean for datascience
    process_for_datascience(dataset_folder)

    # Clean parquet folder
    clean_folder(parquet_folder)


if __name__ == "__main__":
    main()
