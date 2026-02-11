import requests
import json
import os
import time
import schedule
import subprocess
from datetime import datetime

# Configuration
DATA_DIR = "data"
GITHUB_REMOTE = "origin"
GITHUB_BRANCH = "main"

# API Endpoints
ENDPOINTS = {
    "current_schedule": "http://api.jolpi.ca/ergast/f1/current.json",
    "last_results": "http://api.jolpi.ca/ergast/f1/current/last/results.json",
    "current_results": "http://api.jolpi.ca/ergast/f1/current/results.json?limit=100",
    "next_race": "http://api.jolpi.ca/ergast/f1/current/next.json",
    "driver_standings": "http://api.jolpi.ca/ergast/f1/current/driverStandings.json",
    "constructor_standings": "http://api.jolpi.ca/ergast/f1/current/constructorStandings.json",
    "drivers": "http://api.jolpi.ca/ergast/f1/current/drivers.json?limit=100",
    "constructors": "http://api.jolpi.ca/ergast/f1/current/constructors.json?limit=100"
}

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def fetch_and_save(name, url):
    try:
        print(f"Fetching {name} from {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        file_path = os.path.join(DATA_DIR, f"{name}.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved {name} to {file_path}")
        return True
    except Exception as e:
        print(f"Error fetching {name}: {e}")
        return False


def git_commit_and_push():
    try:
        # Detect current branch
        branch = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True).stdout.strip()
        if not branch:
            branch = "main" # Fallback

        # Check if there are changes
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not status.stdout.strip():
            print("No changes to commit.")
            return

        print(f"Changes detected on branch '{branch}'. Committing and pushing...")
        
        # Add changes
        subprocess.run(["git", "add", "data/*.json"], check=True)
        
        # Commit
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(["git", "commit", "-m", f"Auto-update F1 data: {timestamp}"], check=True)

        # Pull latest changes to avoid conflicts (rebase on top of pulled changes)
        try:
            print("Pulling latest changes...")
            subprocess.run(["git", "pull", "--rebase", GITHUB_REMOTE, branch], check=True)
        except subprocess.CalledProcessError:
            print("Pull failed (maybe no remote branch yet?), continuing...")
        
        # Push
        subprocess.run(["git", "push", GITHUB_REMOTE, branch], check=True)
        print("Successfully pushed to GitHub.")
        
    except subprocess.CalledProcessError as e:
        print(f"Git operation failed: {e}")

def job():
    print(f"Starting job at {datetime.now()}")
    ensure_data_dir()
    
    success_count = 0
    for name, url in ENDPOINTS.items():
        if fetch_and_save(name, url):
            success_count += 1
            
    if success_count > 0:
        git_commit_and_push()
    
    print("Job finished.")

def main():
    print("F1 Data Fetcher started...")
    
    # Configure git user if not present (system level or repo level)
    subprocess.run(["git", "config", "user.name", "F1 Data Bot"], check=False)
    subprocess.run(["git", "config", "user.email", "bot@planc.com"], check=False)
    
    # Run once immediately
    job()
    
    # Schedule every hour
    schedule.every(1).hours.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
