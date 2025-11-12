FROM python:3.11-slim

LABEL maintainer="Resilient UCSD"
LABEL description="Netlify Deployment Approval Workflow Slack Bot"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY version.py .
COPY manifest.json .
COPY LICENSE .

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV WEBHOOK_PORT=5000

# Expose webhook port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=120s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/health -o /dev/null || exit 1

# Run the application
CMD ["python", "app.py"]
