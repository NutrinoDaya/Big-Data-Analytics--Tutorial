from pydantic import BaseModel, Field

# This response model is used for the /analytics/summary endpoint.
# It defines the structure for the overall trip statistics.
class SummaryStats(BaseModel):
    """
    Pydantic model for the summary statistics API response.
    It ensures the output from the analytics function is cast to the correct
    data types for the JSON response.
    """
    total_trips: int = Field(..., description="The total number of trips recorded.", example=150000)
    total_distance: float = Field(..., description="The sum of the distance for all trips in miles.", example=450000.75)
    average_distance: float = Field(..., description="The average distance of a single trip in miles.", example=3.0)
    max_distance: float = Field(..., description="The longest trip distance recorded in miles.", example=55.5)
    total_revenue: float = Field(..., description="The sum of the total fare amount for all trips.", example=2250000.50)
    average_fare: float = Field(..., description="The average fare for a single trip.", example=15.0)
    average_passengers: float = Field(..., description="The average number of passengers per trip.", example=1.6)

    class Config:
        """
        Pydantic V2 configuration.
        'from_attributes = True' is the new name for 'orm_mode = True',
        allowing the model to be populated from object attributes.
        """
        from_attributes = True


# This response model is used for the /analytics/top-pickup-locations endpoint.
class TopLocation(BaseModel):
    """
    Pydantic model for a single top pickup location.
    The API will return a list of these objects.
    """
    pickup_latitude: float = Field(..., description="The latitude of the pickup location.", example=40.767937)
    pickup_longitude: float = Field(..., description="The longitude of the pickup location.", example=-73.982155)
    trip_count: int = Field(..., description="The number of trips originating from this location.", example=520)

    class Config:
        """
        Pydantic V2 configuration.
        'from_attributes = True' is the new name for 'orm_mode = True'.
        """
        from_attributes = True