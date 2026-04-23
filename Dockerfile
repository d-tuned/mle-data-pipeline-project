# Start from an official Python image
# slim means it's a minimal version — smaller and faster to download
FROM python:3.11-slim

# Set the working directory inside the container
# All subsequent commands run from here
WORKDIR /app

# Copy dependency files first
# Docker builds in layers — copying requirements before code means
# Docker caches the pip install step and only reruns it if
# requirements change, not every time your code changes
COPY requirements.txt pyproject.toml ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY src/ ./src/

# Install our own package
RUN pip install --no-cache-dir -e .

# Create the data directories
RUN mkdir -p data/raw data/processed

# Default command — run the full pipeline
CMD ["data-pipeline", "all"]