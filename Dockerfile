FROM python:3.9-slim

# Install git and openssh-client for git push operations
RUN apt-get update && apt-get install -y git openssh-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Ensure the scripts directory exists (it should be copied, but good to be safe)
# The ENTRYPOINT assumes the script is locally available in /app/scripts/fetch_f1_data.py
# Use -u for unbuffered output to see logs in real-time
CMD ["python", "-u", "scripts/fetch_f1_data.py"]
