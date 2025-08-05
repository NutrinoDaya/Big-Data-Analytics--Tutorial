from pydantic import BaseModel, Field
from datetime import datetime

class TripData(BaseModel):
    """
    Pydantic model for incoming taxi trip data via the API.
    """
    vendor_id: int
    pickup_datetime: datetime
    dropoff_datetime: datetime
    passenger_count: int = Field(..., gt=0)
    trip_distance: float = Field(..., gt=0)
    pickup_longitude: float
    pickup_latitude: float
    dropoff_longitude: float
    dropoff_latitude: float
    total_amount: float = Field(..., gt=0)

    class Config:
        """
        Pydantic V2 configuration.
        'json_schema_extra' is the new name for 'schema_extra'.
        """
        json_schema_extra = {
            "example": {
                "vendor_id": 1,
                "pickup_datetime": "2025-08-06T09:30:00Z",
                "dropoff_datetime": "2025-08-06T09:45:00Z",
                "passenger_count": 1,
                "trip_distance": 2.5,
                "pickup_longitude": -73.982155,
                "pickup_latitude": 40.767937,
                "dropoff_longitude": -73.96463,
                "dropoff_latitude": 40.765602,
                "total_amount": 15.5
            }
        }