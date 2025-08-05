import requests
import time
import random
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# FastAPI endpoint URL
API_URL = "http://localhost:8000/ingest/trip"

# --- MODIFIED: New simulation parameters ---
RECORDS_PER_BATCH = 200
BATCH_INTERVAL_SECONDS = 5
SIMULATION_DURATION_MINUTES = 60 # Run for an hour

def generate_trip_data():
    """Generates a single dummy taxi trip record."""
    # Using the current time to be more realistic for a stream
    pickup_datetime = datetime.now() - timedelta(seconds=random.randint(0, 300))
    dropoff_datetime = pickup_datetime + timedelta(minutes=random.randint(5, 45))
    
    return {
        "vendor_id": random.choice([1, 2]),
        "pickup_datetime": pickup_datetime.isoformat(),
        "dropoff_datetime": dropoff_datetime.isoformat(),
        "passenger_count": random.randint(1, 6),
        "trip_distance": round(random.uniform(0.5, 10.0), 2),
        "pickup_longitude": round(random.uniform(-74.01, -73.93), 6),
        "pickup_latitude": round(random.uniform(40.70, 40.80), 6),
        "dropoff_longitude": round(random.uniform(-74.01, -73.93), 6),
        "dropoff_latitude": round(random.uniform(40.70, 40.80), 6),
        "total_amount": round(random.uniform(5.0, 80.0), 2)
    }

def run_generator():
    """
    Generates and sends a batch of records to the API endpoint
    at a regular interval.
    """
    end_time = time.time() + SIMULATION_DURATION_MINUTES * 60
    
    logging.info(
        f"Starting data generation: {RECORDS_PER_BATCH} records every {BATCH_INTERVAL_SECONDS} seconds "
        f"for {SIMULATION_DURATION_MINUTES} minutes."
    )
    
    while time.time() < end_time:
        logging.info(f"--- Starting new batch of {RECORDS_PER_BATCH} records ---")
        try:
            # Loop to send a burst of records
            for i in range(RECORDS_PER_BATCH):
                data = generate_trip_data()
                response = requests.post(API_URL, json=data, timeout=5)
                
                # Optional: Log progress without being too spammy
                if (i + 1) % 50 == 0:
                     logging.info(f"Sent record {i + 1}/{RECORDS_PER_BATCH}...")

            logging.info(f"Batch of {RECORDS_PER_BATCH} complete. Waiting for {BATCH_INTERVAL_SECONDS} seconds.")
            
        except requests.exceptions.RequestException as e:
            logging.error(f"A request in the batch failed: {e}")
            logging.info("Waiting before starting next batch...")
        except KeyboardInterrupt:
            logging.info("Data generator stopped by user.")
            break

        # Wait for the specified interval before starting the next batch
        time.sleep(BATCH_INTERVAL_SECONDS)
            
    logging.info("Simulation finished.")

if __name__ == "__main__":
    # Wait for the API server to be ready
    logging.info("Waiting 10 seconds for API server to start...")
    time.sleep(10) 
    run_generator()