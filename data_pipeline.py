import os
import glob
import logging
import time

import pandas as pd
import sqlite3

from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.provider_sensor import ProviderSensor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class LoggerSetup:
    @staticmethod
    def setup_logger():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '[%(asctime)s][%(name)s][%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        return logger


class DatabaseManager:
    def __init__(self, db_path='processed_files.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL UNIQUE
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS dataset (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )
        ''')
        conn.commit()
        conn.close()

    def is_file_processed(self, file_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM processed_files WHERE file_name = ?', (file_name,))
        result = cursor.fetchone()[0]
        conn.close()
        return result > 0

    def mark_file_as_processed(self, file_name):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO processed_files (file_name) VALUES (?)', (file_name,))
        conn.commit()
        conn.close()

    def add_missing_columns(self, df):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        df_columns = set(df.columns)
        cursor.execute("PRAGMA table_info(dataset)")
        existing_columns = set([row[1] for row in cursor.fetchall()])
        missing_columns = df_columns - existing_columns

        for column in missing_columns:
            cursor.execute(f'ALTER TABLE dataset ADD COLUMN {column} TEXT')
        conn.commit()
        conn.close()

    def store_in_db(self, df):
        conn = sqlite3.connect(self.db_path)
        self.add_missing_columns(df)
        df.to_sql('dataset', conn, if_exists='append', index=False)
        conn.close()


class FocusConverterService:
    def __init__(self, output_folder, db_manager):
        self.output_folder = output_folder
        self.db_manager = db_manager

    def convert_csv_to_focus(self, file_path):
        if self.db_manager.is_file_processed(file_path):
            logger.info(f"File {file_path} has already been processed. Skipping.")
            raise Exception(f"File {file_path} has already been processed. Skipping.")

        logger.info(f"Converting CSV file {file_path} to FOCUS format and saving it in {self.output_folder}")
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
                export_path=self.output_folder,
                export_include_source_columns=True,
                basename_template=None,
            )
            converter.prepare_horizontal_conversion_plan(provider=provider_sensor.provider)
            converter.convert()
            logger.info(f"File {file_path} converted successfully")
            self.db_manager.mark_file_as_processed(file_path)
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


class DatasetBuilder:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def build_dataset(self, dataframes):
        logger.info(f"Building dataset from dataframes and saving them in the database")

        for df in dataframes:
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])

        if dataframes:
            df_concat = pd.concat(dataframes, ignore_index=True)
            self.db_manager.store_in_db(df_concat)


class FileWatcher:
    def __init__(self, directory_to_watch, focus_converter_service, dataset_builder):
        self.DIRECTORY_TO_WATCH = directory_to_watch
        self.focus_converter_service = focus_converter_service
        self.dataset_builder = dataset_builder
        self.observer = Observer()

    def run(self):
        event_handler = Handler(self.focus_converter_service, self.dataset_builder)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            logger.info("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):
    def __init__(self, focus_converter_service, dataset_builder):
        self.focus_converter_service = focus_converter_service
        self.dataset_builder = dataset_builder

    def process(self, event):
        if event.is_directory:
            return None
        elif event.event_type == 'created':
            try:
                logger.info(f"Received created event - {event.src_path}")
                self.focus_converter_service.convert_csv_to_focus(event.src_path)
                df = pd.read_csv(event.src_path)
                self.dataset_builder.build_dataset([df])
            except Exception as e:
                logger.error(f"Error processing {event.src_path}: {e}")

    def on_created(self, event):
        self.process(event)


class Pipeline:
    def __init__(self, input_folder, output_folder):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.db_manager = DatabaseManager()
        self.focus_converter_service = FocusConverterService(output_folder, self.db_manager)
        self.dataset_builder = DatasetBuilder(self.db_manager)
        self.file_watcher = FileWatcher(input_folder, self.focus_converter_service, self.dataset_builder)

    def process_existing_files(self):
        logger.info("Processing existing files in the input folder")
        csv_files = glob.glob(os.path.join(self.input_folder, '*.csv'))
        dataframes = []
        for file_path in csv_files:
            try:
                self.focus_converter_service.convert_csv_to_focus(file_path)
                df = pd.read_csv(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        self.dataset_builder.build_dataset(dataframes)

    def run(self):
        logger.info("====================================")
        logger.info("Starting data pipeline")
        logger.info("Last update: 2024-05-21")
        logger.info("Development phase: WIP")
        logger.info("====================================")

        os.makedirs(self.input_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)

        self.process_existing_files()
        self.file_watcher.run()


if __name__ == "__main__":
    logger = LoggerSetup.setup_logger()
    pipeline = Pipeline(input_folder="input", output_folder="output")
    pipeline.run()
