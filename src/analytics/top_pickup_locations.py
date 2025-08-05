from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from typing import List, Dict
import logging

# Initialize logger
logger = logging.getLogger(__name__)

def get_top_pickup_locations(spark: SparkSession, delta_path: str, limit: int = 10) -> List[Dict]:
    """
    Finds the top N pickup locations based on trip count.

    A "location" is defined by a unique latitude/longitude pair. The function
    groups by these coordinates, counts the trips, and returns the most frequent ones.

    Args:
        spark (SparkSession): The active Spark session.
        delta_path (str): The path to the Delta Lake table containing trip data.
        limit (int, optional): The number of top locations to return. Defaults to 10.

    Returns:
        List[Dict]: A list of dictionaries, where each dictionary represents a
                    top pickup location and its trip count. Returns an empty list
                    if the table is empty or an error occurs.
    """
    try:
        logger.info(f"Reading Delta table from: {delta_path} to find top {limit} pickup locations.")
        df = spark.read.format("delta").load(delta_path)
        
        if df.rdd.isEmpty():
            logger.warning("The Delta table is empty. Returning empty list for top locations.")
            return []

        logger.info("Grouping by pickup location and counting trips...")
        
        # Group by location, count trips, sort by count descending, and take the top N.
        # Use .alias() to name the count column 'trip_count' to match the Pydantic model.
        top_locations_df = df.groupBy("pickup_latitude", "pickup_longitude") \
                             .agg(count("*").alias("trip_count")) \
                             .orderBy(col("trip_count").desc()) \
                             .limit(limit)

        # Collect the results from the Spark cluster to the driver as a list of dictionaries.
        results = [row.asDict() for row in top_locations_df.collect()]
        
        logger.info(f"Successfully identified top {len(results)} pickup locations.")
        return results

    except Exception as e:
        logger.error(f"Error finding top pickup locations from {delta_path}: {e}", exc_info=True)
        # Re-raise the exception to be handled by the API layer
        raise