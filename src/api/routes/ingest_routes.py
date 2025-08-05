from fastapi import APIRouter, Request, BackgroundTasks, status, Response
from typing import List
import threading

from src.api.models.request_models import TripData
from src.etl.processor.streaming_processor import process_micro_batch

router = APIRouter()

# --- In-Memory Buffer and Lock ---
# Using a simple list as a buffer. For high-concurrency, collections.deque is more efficient.
TRIP_BUFFER: List[TripData] = []
# A lock is crucial because FastAPI can handle multiple requests concurrently in threads
BUFFER_LOCK = threading.Lock()
# Define the threshold for processing
BUFFER_THRESHOLD = 200

@router.post("/trip", status_code=status.HTTP_202_ACCEPTED)
async def ingest_trip(trip: TripData, request: Request, background_tasks: BackgroundTasks):
    """
    Ingests a single taxi trip record and adds it to a buffer.
    When the buffer reaches a threshold, it triggers a background task
    to process the batch with Spark.
    """
    global TRIP_BUFFER
    
    batch_to_process = []
    
    with BUFFER_LOCK:
        TRIP_BUFFER.append(trip.dict())
        # Check if the buffer has reached the threshold
        if len(TRIP_BUFFER) >= BUFFER_THRESHOLD:
            # Move data to a temporary list for processing
            batch_to_process = list(TRIP_BUFFER)
            # Clear the main buffer immediately to prevent it from growing
            # and to start accepting new records.
            TRIP_BUFFER.clear()

    if batch_to_process:
        # Get the Spark session and paths from the application state
        spark = request.app.state.spark
        paths = request.app.state.paths_config
        delta_path = paths['delta_lake_path']
        
        # Add the Spark processing job to the background
        background_tasks.add_task(process_micro_batch, spark, batch_to_process, delta_path)
        return {"message": "Batch threshold reached. Processing started in the background."}
        
    return {"message": "Trip data accepted and buffered."}