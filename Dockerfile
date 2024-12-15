# Use an official Python image as the base image
FROM python:3.11-slim

# Set environment variables to prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a working directory
WORKDIR /app

# Copy the project files to the container
COPY . /app

# Install uv
RUN pip install --no-cache-dir uv

# Use uv to build the package
RUN uv build

# Create a directory to store the built wheel (already in dist via uv)
RUN mkdir -p /output

# Copy the built wheel to a separate directory for easier access
RUN cp dist/*.whl /output/

# Set the entrypoint to inspect the output, if needed (optional)
CMD ["ls", "-l", "/output"]

