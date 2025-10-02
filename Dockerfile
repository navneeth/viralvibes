# Base image
FROM python:3.11-slim

WORKDIR /app

# Install ffmpeg and curl (yt-dlp needs ffmpeg)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements-backend.txt .
RUN pip install --no-cache-dir -r requirements-backend.txt

# Copy app source
COPY . .

# Entrypoint
RUN chmod +x /app/fly-entrypoint.sh
ENTRYPOINT ["/app/fly-entrypoint.sh"]
