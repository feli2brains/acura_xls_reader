# Multi-stage Dockerfile for XLSX Reader Microkernel

# Stage 1: Development environment
FROM python:3.11-slim as development

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Install the package in development mode
RUN pip install -e .

# Stage 2: Production environment
FROM python:3.11-slim as production

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash app

# Set work directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Install the package
RUN pip install .

# Change ownership to app user
RUN chown -R app:app /app

# Switch to app user
USER app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import xls_reader; print('Health check passed')" || exit 1

# Default command
CMD ["python", "-m", "xls_reader.cli"]

# Stage 3: Testing environment
FROM development as testing

# Install testing dependencies
RUN pip install pytest pytest-cov pytest-mock

# Set environment for testing
ENV PYTHONPATH=/app
ENV TESTING=1

# Default command for testing
CMD ["pytest", "tests/", "-v", "--cov=xls_reader", "--cov-report=html"] 