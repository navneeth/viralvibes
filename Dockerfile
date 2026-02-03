FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install Python deps
COPY requirements-backend.txt .
RUN pip install --no-cache-dir -r requirements-backend.txt

# Copy only what worker needs (optional refinement later)
COPY . .

# 🚨 No entrypoint script
CMD ["python", "-m", "worker.worker"]
