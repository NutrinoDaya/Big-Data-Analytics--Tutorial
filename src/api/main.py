import uvicorn
from fastapi import FastAPI
from pyspark.sql import SparkSession
from contextlib import asynccontextmanager
from delta import configure_spark_with_delta_pip  # <-- STEP 1: IMPORT THE MAGIC HELPER

# Using the relative imports you have in your file
from ..config_loader.load_config import load_all_configs
from .routes import analytics_routes, ingest_routes
from ..utils.logger import setup_logger

# Load configurations
configs = load_all_configs()
api_config = configs['api']
spark_config = configs['spark']
paths_config = configs['paths']

# Setup logger
logger = setup_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager for the FastAPI application.
    Handles startup and shutdown events, such as initializing and stopping the Spark session.
    """
    logger.info("Starting up: Initializing Spark session...")
    
    # Start building the Spark session as before
    spark_builder = SparkSession.builder \
        .appName(spark_config['app_name']) \
        .master(spark_config['master'])

    # Loop through our custom configs (extensions, catalog, etc.)
    for key, value in spark_config['delta_catalog'].items():
        spark_builder = spark_builder.config(key, value)
    
    # --- STEP 2: APPLY THE DELTA CONFIGURATIONS ---
    logger.info("Configuring Spark with Delta Lake support...")
    spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

    spark.sparkContext.setLogLevel(spark_config['log_level'])
    
    # Mount the Spark session and configs to the application state
    app.state.spark = spark
    app.state.paths_config = paths_config
    
    logger.info("Spark session initialized and attached to app state.")
    
    yield
    
    # --- Shutdown Logic ---
    logger.info("Shutting down: Stopping Spark session...")
    if hasattr(app.state, 'spark') and app.state.spark:
        app.state.spark.stop()
        logger.info("Spark session stopped successfully.")

# Create FastAPI app with the lifespan manager
app = FastAPI(
    lifespan=lifespan,
    title=api_config['title'],
    description=f"{api_config['description']} Now with streaming ingestion.",
    version=api_config['version']
)

# --- Include Routers ---
app.include_router(ingest_routes.router, prefix="/ingest", tags=["Data Ingestion"])
app.include_router(analytics_routes.router, prefix="/analytics", tags=["Analytics"])