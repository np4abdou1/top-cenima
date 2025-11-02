FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory for the database
RUN mkdir -p /app/data

# Expose the port the app runs on
EXPOSE 8080

# Run the application
CMD ["python", "app.py"]
