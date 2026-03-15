# Use Python 3.9 slim
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    openssh-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Ensure git doesn't complain about directory ownership in volumes
RUN git config --global --add safe.directory /app

CMD ["python", "-u", "scripts/fetch_f1_data.py"]
