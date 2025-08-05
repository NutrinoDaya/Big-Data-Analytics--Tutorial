from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import IntegerType, DoubleType
import logging

logger = logging.getLogger(__name__)

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforms the raw taxi trip DataFrame.

    - Converts string timestamps to timestamp type.
    - Casts numeric columns to their proper types.
    - Creates date/time partition columns (year, month, day).
    - Filters out invalid or unreasonable records.

    Args:
        df (DataFrame): The raw input DataFrame from an ingested micro-batch.

    Returns:
        DataFrame: The transformed and cleaned DataFrame, ready for loading.
    """
    try:
        logger.info("Starting data transformation...")

        # 1. Type Casting and column selection to ensure schema consistency
        # This handles cases where data might come in with slightly different types
        df_transformed = df.select(
            col("vendor_id").cast(IntegerType()),
            to_timestamp(col("pickup_datetime")).alias("pickup_datetime"),
            to_timestamp(col("dropoff_datetime")).alias("dropoff_datetime"),
            col("passenger_count").cast(IntegerType()),
            col("trip_distance").cast(DoubleType()),
            col("pickup_longitude").cast(DoubleType()),
            col("pickup_latitude").cast(DoubleType()),
            col("dropoff_longitude").cast(DoubleType()),
            col("dropoff_latitude").cast(DoubleType()),
            col("total_amount").cast(DoubleType())
        )

        # 2. Feature Engineering (for partitioning and analysis)
        df_transformed = df_transformed.withColumn("pickup_year", year(col("pickup_datetime"))) \
                                       .withColumn("pickup_month", month(col("pickup_datetime"))) \
                                       .withColumn("pickup_day", dayofmonth(col("pickup_datetime"))) \
                                       .withColumn("pickup_hour", hour(col("pickup_datetime")))

        # 3. Handle null values after casting
        # Drop any rows where key information became null during conversion (e.g., bad timestamp format)
        df_transformed = df_transformed.na.drop(
            subset=["pickup_datetime", "dropoff_datetime", "total_amount", "trip_distance"]
        )

        # 4. Filter out unreasonable values (data quality check)
        # - Trip distance and total amount must be positive.
        # - Trip duration should be positive.
        df_transformed = df_transformed.filter(
            (col("trip_distance") > 0) & 
            (col("total_amount") > 0) &
            (col("dropoff_datetime") > col("pickup_datetime"))
        )

        logger.info("Data transformation completed successfully.")
        return df_transformed
        
    except Exception as e:
        logger.error(f"Error during data transformation: {e}", exc_info=True)
        # Re-raise the exception to be handled by the calling process
        raise