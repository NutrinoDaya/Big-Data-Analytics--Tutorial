# Use an official Python runtime as a parent image
# NOTE: Your requirements use Python 3.11 features, so updating the base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install system dependencies
# We need Java for Spark and procps for utilities like 'ps'
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
COPY . .

# Set Java Home for Spark.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Command to run the application
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]