import json
import datetime
from pathlib import Path
import pandas as pd
import cbsodata as c
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import DATA_DIR, META_DIR

from functions.logger import setup_logger, cleanup_old_logs
from functions.file_utils import create_directory, empty_directory
from functions.df_utils import filter_dataframe, add_metadata_columns

# === PARAMETERIZED CONFIG ===
META_FILE = META_DIR / "bronze/cbs/bronze_cbs_tables.json"
BASE_PATH = DATA_DIR
OUTPUT_FORMAT = "parquet"  # parquet or "csv"

logger = setup_logger()
cleanup_old_logs()


def process_table(table_info: dict, base_path: Path, file_format: str) -> None:
    """
    Downloads, filters, adds metadata, and saves CBS data in the specified format.

    Args:
        table_info (dict): Contains table_name, filter_column, filter_value.
        base_path (Path): Base directory to save files.
        file_format (str): 'parquet' or 'csv'.
    """
    try:
        table_name = table_info.get("table_name")
        filter_column = table_info.get("filter_column")
        filter_value = table_info.get("filter_value")

        if not table_name:
            logger.warning(f"Skipping entry with missing table_name: {table_info}")
            return

        logger.info(f"Downloading: {table_name}")
        table = c.get_data(table_name)
        df = pd.DataFrame(table)

        if filter_column and filter_value:
            if filter_column in df.columns:
                df = filter_dataframe(df, filter_column, filter_value)
                logger.info(f"Filtered {table_name} on column '{filter_column}' with value '{filter_value}'")
            else:
                logger.warning(f"Filter column '{filter_column}' not found in table '{table_name}'. Skipping filtering.")
        else:
            logger.info(f"No filtering applied for {table_name}")

        df = add_metadata_columns(df, table_name)

        # File and path setup
        retrieval_date = datetime.date.today().strftime("%Y%m%d")
        output_dir = base_path / 'bronze' / 'cbs' / table_name / retrieval_date
        create_directory(output_dir)
        empty_directory(output_dir)

        timestamp = datetime.datetime.now().strftime("%H%M%S")
        file_path = output_dir / f"{table_name}_{timestamp}.{file_format}"

        # Save based on format
        if file_format == "parquet":
            df.to_parquet(file_path, index=False)
        elif file_format == "csv":
            df.to_csv(file_path, index=False)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return

        logger.info(f"Saved {len(df)} rows to {file_path}")

    except Exception as e:
        logger.error(f"Error processing tempty_directoryable '{table_info}': {e}", exc_info=True)



def download_cbs_tables_to_parquet(meta_file: str, output_base_path: Path,  output_filetype: str, max_threads: int = 4) -> None:
    try:
        with open(meta_file, "r") as f:
            data = json.load(f)
            tables_meta = data.get("tables", [])

            if not isinstance(tables_meta, list):
                logger.error(f"'tables' should be a list in JSON file: {meta_file}")
                return

        logger.info(f"Processing {len(tables_meta)} tables using {max_threads} threads")

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(process_table, table_info, Path(output_base_path), output_filetype) for table_info in tables_meta]

            for future in as_completed(futures):
                future.result()  # this will raise any exceptions that were thrown

    except Exception as e:
        logger.error(f"Failed to load or process metadata: {e}", exc_info=True)

# Execute python package
if __name__ == "__main__":
    download_cbs_tables_to_parquet(
        meta_file=META_FILE,
        output_base_path=BASE_PATH,
        output_filetype=OUTPUT_FORMAT,
        max_threads=8  # Adjust based on your system
    )
