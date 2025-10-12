FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash bot
USER bot

# Expose port (required for Cloud Run)
EXPOSE 8080

# Start the Discord bot
CMD ["python", "main.py"]