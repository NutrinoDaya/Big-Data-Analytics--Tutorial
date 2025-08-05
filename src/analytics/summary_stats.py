from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, sum, count, max, col
import logging

# Initialize logger
logger = logging.getLogger(__name__)

def get_summary_statistics(spark: SparkSession, delta_path: str) -> dict:
    """
    Calculates summary statistics from the taxi trip Delta table.

    This function performs a single, efficient aggregation pass over the data
    to compute all required metrics.

    Args:
        spark (SparkSession): The active Spark session.
        delta_path (str): The path to the Delta Lake table containing trip data.

    Returns:
        dict: A dictionary containing key summary statistics. Returns an empty
              dictionary if the table is empty or an error occurs.
    """
    try:
        logger.info(f"Reading Delta table from: {delta_path}")
        df = spark.read.format("delta").load(delta_path)

        if df.rdd.isEmpty():
            logger.warning("The Delta table is empty. Returning zero-value statistics.")
            return {
                "total_trips": 0, "total_distance": 0.0, "average_distance": 0.0,
                "max_distance": 0.0, "total_revenue": 0.0, "average_fare": 0.0,
                "average_passengers": 0.0
            }

        logger.info("Calculating summary statistics...")
        
        # Perform all aggregations in a single pass for efficiency.
        # Use .alias() to ensure the output column names match the Pydantic response model.
        stats_df = df.agg(
            count("*").alias("total_trips"),
            sum("trip_distance").alias("total_distance"),
            avg("trip_distance").alias("average_distance"),
            max("trip_distance").alias("max_distance"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("average_fare"),
            avg("passenger_count").alias("average_passengers")
        )

        # The result is a DataFrame with one row. Collect it and convert to a dictionary.
        stats_dict = stats_df.first().asDict()
        
        logger.info("Successfully calculated summary statistics.")
        return stats_dict

    except Exception as e:
        logger.error(f"Error calculating summary statistics from {delta_path}: {e}", exc_info=True)
        # Re-raise the exception to be handled by the API layer
        raise