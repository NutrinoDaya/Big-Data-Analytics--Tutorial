from pyspark.sql import SparkSession
from typing import List, Dict
import logging

from src.etl.transform import transform_data
from src.etl.load import load_to_delta

logger = logging.getLogger(__name__)

def process_micro_batch(spark: SparkSession, batch_data: List[Dict], delta_path: str):
    """
    Processes a micro-batch of data from the in-memory buffer.
    
    1. Converts the list of dictionaries to a Spark DataFrame.
    2. Transforms the DataFrame.
    3. Appends the transformed data to the Delta Lake table.

    Args:
        spark (SparkSession): The active Spark session.
        batch_data (List[Dict]): The list of records to process.
        delta_path (str): The path to the target Delta Lake table.
    """
    if not batch_data:
        logger.info("process_micro_batch called with no data. Skipping.")
        return

    try:
        logger.info(f"Processing a micro-batch of {len(batch_data)} records.")
        
        # 1. Create DataFrame from the in-memory list
        # We let Spark infer the schema from the first record, which is efficient for this structure.
        raw_df = spark.createDataFrame(batch_data)
        
        # 2. Transform the data
        transformed_df = transform_data(raw_df)
        
        # 3. Load (append) to Delta Lake
        partition_cols = ["pickup_year", "pickup_month", "pickup_day"]
        load_to_delta(transformed_df, delta_path, partition_cols=partition_cols, mode="append")
        
        logger.info(f"Successfully processed and appended {transformed_df.count()} records to Delta table.")

    except Exception as e:
        logger.error(f"Failed to process micro-batch: {e}", exc_info=True)