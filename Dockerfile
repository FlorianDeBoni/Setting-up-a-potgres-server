# Dockerfile
FROM python:3.12-slim

# Install system dependencies (psycopg2, etc.)
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy your Python utility code
COPY utils /app

# Default command: open shell for debugging
CMD ["bash"]
