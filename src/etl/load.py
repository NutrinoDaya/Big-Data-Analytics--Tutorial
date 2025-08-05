from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def load_to_delta(df: DataFrame, path: str, partition_cols: list = None, mode: str = "overwrite"):
    """
    Saves a DataFrame to a Delta Lake table with specified mode and partitioning.

    This function is idempotent for 'overwrite' mode and appends data for 'append' mode,
    making it suitable for both batch and streaming workloads.

    Args:
        df (DataFrame): The DataFrame to save.
        path (str): The file system path to the Delta Lake table.
        partition_cols (list, optional): A list of column names to partition the data by. 
                                         Partitioning improves query performance. Defaults to None.
        mode (str, optional): The save mode. Can be 'overwrite', 'append', 'ignore', 'errorifexists'.
                              Defaults to "overwrite". For streaming, 'append' is used.
    """
    try:
        if df.rdd.isEmpty():
            logger.warning("The DataFrame to load is empty. Skipping the load operation.")
            return

        logger.info(f"Loading data to Delta Lake at '{path}' in '{mode}' mode...")

        writer = df.write.format("delta").mode(mode)

        if partition_cols and isinstance(partition_cols, list):
            # The '*' unpacks the list into arguments for the function
            writer = writer.partitionBy(*partition_cols)
            logger.info(f"Data will be partitioned by: {', '.join(partition_cols)}")
        
        # Adding options for schema evolution and optimized writing
        writer = writer.option("mergeSchema", "true") \
                       .option("maxRecordsPerFile", 256 * 1024 * 1024) # Example option for file sizing

        # Execute the save operation
        writer.save(path)
        
        logger.info(f"Successfully loaded data to Delta Lake table at '{path}'.")

    except Exception as e:
        logger.error(f"Failed to load data to Delta Lake at '{path}': {e}", exc_info=True)
        # Re-raise the exception to ensure the calling process is aware of the failure
        raise