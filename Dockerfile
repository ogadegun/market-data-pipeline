# Use Python 3.10 slim image as base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Set environment to unbuffered (see logs in real-time)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app.py"]