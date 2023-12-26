import os
import argparse
import logging
from logging.handlers import RotatingFileHandler
import time
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


class DataProcessor:
    def __init__(self):
        self.logger = self.setup_logging()

    def get_project_root(self):
        """Get the root directory of the project."""
        return os.path.dirname(os.path.abspath(__file__))

    def get_config_path(self, config_file):
        """Get the absolute path to a configuration file."""
        return os.path.join(self.get_project_root(), 'config', config_file)

    def get_logs_path(self, log_file):
        """Get the absolute path to a log file."""
        logs_folder_path = os.path.join(self.get_project_root(), 'logs')
        if not os.path.exists(logs_folder_path):
            os.makedirs(logs_folder_path)
        return os.path.join(logs_folder_path, log_file)

    def load_column_selection_config(self):
        """Load column selection configuration from YAML file."""
        config_path = self.get_config_path('column_selection_config.yaml')
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config

    def filter_data(self, clients_df, countries):
        """Filter data based on specified conditions."""
        try:
            self.logger.info("Filtering data...")
            result_data = clients_df.filter(clients_df.country.isin(countries))
            
            row_count = result_data.count()
            self.logger.info(f"Filtering data completed. Rows: {row_count}")
            return result_data
        except AnalysisException as ae:
            self.logger.error(f"Spark AnalysisException in filter_data: {str(ae)}")
            raise AnalysisException(f"Spark AnalysisException in filter_data: {str(ae)}")

    def load_column_rename_config(self):
        """Load column renaming configuration from YAML file."""
        config_path = self.get_config_path('column_rename_config.yaml')
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config

    def rename_columns(self, data_df):
        """Rename columns in the DataFrame based on the configuration file."""
        self.logger.info("Renaming columns...")
        column_rename_config = self.load_column_rename_config()

        for rename_entry in column_rename_config.get('renamed_columns', []):
            original_name = rename_entry.get('original_name')
            new_name = rename_entry.get('new_name')

            if original_name and new_name:
                data_df = data_df.withColumnRenamed(original_name, new_name)

        self.logger.info("Column renaming completed.")
        return data_df

    def join_datasets(self, clients_df, financials_df):
        """Join client and financial datasets without explicit column renaming."""
        try:
            self.logger.info("Joining datasets...")
            result_data = clients_df.join(financials_df, clients_df.id == financials_df.id).drop(financials_df.id)

            row_count = result_data.count()
            self.logger.info(f"Joining datasets completed. Rows: {row_count}")
            return result_data
        except AnalysisException as ae:
            self.logger.error(f"Spark AnalysisException in join_datasets: {str(ae)}")
            raise AnalysisException(f"Spark AnalysisException in join_datasets: {str(ae)}")

    def create_spark_session(self, app_name="DataProcessor"):
        """Create a Spark session."""
        self.logger.info("Creating Spark session...")
        return SparkSession.builder.appName(app_name).getOrCreate()

    def setup_logging(self):
        """Set up logging configuration based on a YAML file."""
        logging_config_path = self.get_config_path('logging_config.yaml')
        with open(logging_config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)

        logs_path = self.get_logs_path('BitcoinTrading.log')
        logs_formatter = config['logs_formatter']
        time_formatter = config['time_formatter']

        file_handler_config = config.get('file_handler', {})
        file_handler_mode = file_handler_config.get('mode',)
        file_handler_max_bytes = file_handler_config.get('max_bytes')
        file_handler_backup_count = file_handler_config.get('backup_count')

        log_handler = logging.handlers.RotatingFileHandler(
            filename=logs_path,
            mode=file_handler_mode,
            maxBytes=file_handler_max_bytes,
            backupCount=file_handler_backup_count
        )

        formatter = logging.Formatter(logs_formatter, time_formatter)
        formatter.converter = time.gmtime
        log_handler.setFormatter(formatter)

        logger = logging.getLogger("BitcoinTrading rotating Log")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(log_handler)

        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

        logger.info("Logging initialized.")
        return logger

    def read_csv_file(self, file_path, description, selected_columns=None):
        """Read a CSV file into a Spark DataFrame and select specific columns."""
        try:
            self.logger.info(f"Reading {description} file...")
            
            if selected_columns:
                data = (
                    SparkSession.builder
                    .appName("ReadFile")
                    .getOrCreate()
                    .read.csv(file_path, header=True, inferSchema=True)
                    .select(selected_columns)
                )
            else:
                data = SparkSession.builder.appName("ReadFile").getOrCreate().read.csv(file_path, header=True, inferSchema=True)

            row_count = data.count()
            self.logger.debug(f"{description} Data - Rows: {row_count}")
            self.logger.info(f"Reading {description} file completed. Rows: {row_count}")
            return data
        except Exception as file_read_error:
            error_message = f"Error reading {description} file: {str(file_read_error)}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def save_to_file(self, data_df, file_path):
        """Save DataFrame to a CSV file."""
        try:
            self.logger.info(f"Saving data to {file_path}...")
            data_df_pandas = data_df.toPandas()
            data_df_pandas.to_csv(file_path, index=False)
            self.logger.info(f"Saving data to {file_path} completed successfully.")
        except Exception as save_error:
            error_message = f"Error saving data to {file_path}: {str(save_error)}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def process_data(self, clients_file, financials_file, countries):
        """Process client and financial data and perform filtering, joining, and renaming."""
        try:
            spark = self.create_spark_session()
            column_selection_config = self.load_column_selection_config()
            clients_columns = column_selection_config.get('clients_columns', [])
            clients = self.read_csv_file(clients_file, "Clients", clients_columns)
            financials_columns = column_selection_config.get('financials_columns', [])
            financials = self.read_csv_file(financials_file, "Financials", financials_columns)
            filtered_data = self.filter_data(clients, countries)
            joined_data = self.join_datasets(filtered_data, financials)
            result_data = self.rename_columns(joined_data)
            result_data.show()
            result_data_no_id = result_data.drop("id", axis=1) if "id" in result_data.columns else result_data
            self.save_to_file(result_data_no_id, "F:/abn/pyspark_assignment/client_data/result_data.csv")
            self.logger.info("Data processing completed successfully.")
        except AnalysisException as ae:
            self.logger.error(f"Spark AnalysisException: {str(ae)}")
            raise AnalysisException(f"Spark AnalysisException: {str(ae)}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise Exception(f"Unexpected error: {str(e)}")

    def main(self):
        """Main entry point for the script."""
        print("Start")
        print(__file__)

        parser = argparse.ArgumentParser(description="Process client data.")
        parser.add_argument("--clients_file", required=True, help="Path to clients dataset file")
        parser.add_argument("--financials_file", required=True, help="Path to financials dataset file")
        parser.add_argument("--countries", required=False, nargs='+', help="List of countries to filter")

        args = parser.parse_args()

        countries_to_filter = args.countries if args.countries else []

        self.process_data(args.clients_file, args.financials_file, countries_to_filter)


if __name__ == "__main__":
    processor = DataProcessor()
    processor.main()
