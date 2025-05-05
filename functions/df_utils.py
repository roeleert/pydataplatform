import datetime
import logging

import pandas as pd

logger = logging.getLogger("CBSLogger")


def filter_dataframe(df: pd.DataFrame, column_name: str, value) -> pd.DataFrame:
    try:
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in DataFrame.")

        if not isinstance(value, (list, pd.Series)):
            value = [value]

        df[column_name] = df[column_name].astype(str).str.strip()
        filtered_df = df[df[column_name].isin(value)]
        logger.debug(f"Filtering complete: {len(filtered_df)} rows matched in column '{column_name}'.")
        return filtered_df

    except Exception as e:
        logger.error(f"Error filtering DataFrame on column '{column_name}': {e}", exc_info=True)
        return pd.DataFrame()


def add_metadata_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        df_copy = df.copy()
        df_copy['src_table'] = table_name
        df_copy['load_date'] = datetime.datetime.now()
        logger.info(
            f"Metadata columns added: 'src_table' = '{table_name}', 'load_date' = '{df_copy['load_date'].iloc[0]}'"
        )
        return df_copy
    except Exception as e:
        logger.error(f"Error adding metadata columns: {e}", exc_info=True)
        return df
