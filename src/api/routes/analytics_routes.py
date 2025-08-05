from fastapi import APIRouter, Depends, Request, HTTPException, Query
from pyspark.sql import SparkSession
from typing import List

# Internal imports from other parts of our project
from src.analytics.summary_stats import get_summary_statistics
from src.analytics.top_pickup_locations import get_top_pickup_locations
from src.api.models.response_models import SummaryStats, TopLocation
from src.utils.logger import setup_logger

# Initialize router and logger
router = APIRouter()
logger = setup_logger()

# --- Dependency Injection Functions ---

def get_spark_session(request: Request) -> SparkSession:
    """
    A dependency that retrieves the active Spark session from the application state.
    """
    return request.app.state.spark

def get_paths(request: Request) -> dict:
    """
    A dependency that retrieves the path configurations from the application state.
    """
    return request.app.state.paths_config


# --- API Endpoints ---

@router.get(
    "/summary",
    response_model=SummaryStats,
    summary="Get Summary Statistics",
    description="Calculates and returns overall summary statistics for all taxi trips stored in the Delta Lake.",
)
def get_summary(
    spark: SparkSession = Depends(get_spark_session),
    paths: dict = Depends(get_paths)
):
    """
    Endpoint to get summary statistics of all taxi trips.
    - **total_trips**: Total number of trips recorded.
    - **total_revenue**: Sum of total_amount for all trips.
    - **average_fare**: Average fare per trip.
    - **average_distance**: Average distance in miles per trip.
    """
    try:
        delta_path = paths['delta_lake_path']
        logger.info(f"Request received for summary statistics from Delta table at: {delta_path}")
        stats = get_summary_statistics(spark, delta_path)
        return stats
    except Exception as e:
        logger.error(f"Error processing /summary request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while processing summary statistics.")

@router.get(
    "/top-pickup-locations",
    response_model=List[TopLocation],
    summary="Get Top N Pickup Locations",
    description="Finds the most frequent pickup locations based on trip count, defined by latitude and longitude pairs.",
)
def get_top_pickups(
    limit: int = Query(default=10, gt=0, le=100, description="The number of top locations to return."),
    spark: SparkSession = Depends(get_spark_session),
    paths: dict = Depends(get_paths)
):
    """
    Endpoint to get the top N pickup locations.
    - **limit**: A query parameter to specify how many top locations to fetch.
    """
    try:
        delta_path = paths['delta_lake_path']
        logger.info(f"Request received for top {limit} pickup locations from Delta table at: {delta_path}")
        top_locations = get_top_pickup_locations(spark, delta_path, limit)
        return top_locations
    except Exception as e:
        logger.error(f"Error processing /top-pickup-locations request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while fetching top pickup locations.")