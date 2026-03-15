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
    "current_schedule": "https://api.jolpi.ca/ergast/f1/current/",
    "last_results": "https://api.jolpi.ca/ergast/f1/current/last/results/",
    "current_results": "https://api.jolpi.ca/ergast/f1/current/results/?limit=100",
    "next_race": "https://api.jolpi.ca/ergast/f1/current/next/",
    "driver_standings": "https://api.jolpi.ca/ergast/f1/current/driverStandings/",
    "constructor_standings": "https://api.jolpi.ca/ergast/f1/current/constructorStandings/",
    "drivers": "https://api.jolpi.ca/ergast/f1/current/drivers/?limit=100",
    "constructors": "https://api.jolpi.ca/ergast/f1/current/constructors/?limit=100",
    "qualifying": "https://api.jolpi.ca/ergast/f1/current/qualifying/?limit=100",
    "sprint": "https://api.jolpi.ca/ergast/f1/current/sprint/?limit=100"
}

def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def fetch_and_save(name, url, retries=3, backoff=15):
    """Fetch JSON from url and save to data dir. Retries on 429 with backoff."""
    for attempt in range(1, retries + 1):
        try:
            print(f"Fetching {name} from {url}...")
            response = requests.get(url, timeout=10)
            if response.status_code == 429 and attempt < retries:
                wait = backoff * attempt
                print(f"  Rate limited (429) for {name}. Retrying in {wait}s... (attempt {attempt}/{retries})")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            file_path = os.path.join(DATA_DIR, f"{name}.json")
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Saved {name} to {file_path}")
            return data
        except Exception as e:
            if attempt < retries:
                wait = backoff * attempt
                print(f"  Error fetching {name}: {e}. Retrying in {wait}s... (attempt {attempt}/{retries})")
                time.sleep(wait)
            else:
                print(f"Error fetching {name}: {e}")
                return None

def transform_result(entry, is_qualy=False):
    """
    Ensures every result object has the mandatory 7 fields required by the Swift app.
    Fields: number, position, positionText, points, grid, laps, status
    All fields MUST be strings and never null.
    """
    def sanitize(val, default):
        if val is None or str(val).strip() == "":
            return default
        return str(val)

    # Basic extraction with fallback
    number = sanitize(entry.get("number"), "")
    position = sanitize(entry.get("position"), "")
    positionText = sanitize(entry.get("positionText"), position)
    status = sanitize(entry.get("status"), "Finished")

    if is_qualy:
        # Qualifying & Sprint Qualifying specific rules
        points = "0"
        grid = ""
        laps = ""
    else:
        # Race & Sprint specific rules
        points = sanitize(entry.get("points"), "0")
        grid = sanitize(entry.get("grid"), "0")
        laps = sanitize(entry.get("laps"), "0")

    return {
        "number": number,
        "position": position,
        "positionText": positionText,
        "points": points,
        "grid": grid,
        "laps": laps,
        "status": status,
        "Driver": entry.get("Driver", {}),
        "Constructor": entry.get("Constructor", {})
    }

def merge_results_into_schedule():
    """
    Consolidates Qualifying, Race, Sprint, and Sprint Qualifying results into current_schedule.json.
    Each result is transformed to match the Swift app's 7-field requirement.
    """
    print("Merging results into current_schedule.json...")
    try:
        schedule_path = os.path.join(DATA_DIR, "current_schedule.json")
        if not os.path.exists(schedule_path):
            print("  Schedule file not found.")
            return

        with open(schedule_path, "r") as f:
            sched_data = json.load(f)

        races = sched_data["MRData"]["RaceTable"]["Races"]

        # Map Results (Race)
        results_path = os.path.join(DATA_DIR, "current_results.json")
        results_map = {}
        if os.path.exists(results_path):
            with open(results_path, "r") as f:
                res_data = json.load(f)
                for race in res_data["MRData"]["RaceTable"]["Races"]:
                    rnd = race.get("round")
                    if rnd:
                        results_map[rnd] = [transform_result(r, is_qualy=False) for r in race.get("Results", [])]

        # Map Qualifying
        qual_path = os.path.join(DATA_DIR, "qualifying.json")
        qual_map = {}
        if os.path.exists(qual_path):
            with open(qual_path, "r") as f:
                q_data = json.load(f)
                for race in q_data["MRData"]["RaceTable"]["Races"]:
                    rnd = race.get("round")
                    if rnd:
                        qual_map[rnd] = [transform_result(r, is_qualy=True) for r in race.get("QualifyingResults", [])]

        # Map Sprint
        sprint_path = os.path.join(DATA_DIR, "sprint.json")
        sprint_map = {}
        if os.path.exists(sprint_path):
            with open(sprint_path, "r") as f:
                s_data = json.load(f)
                for race in s_data["MRData"]["RaceTable"]["Races"]:
                    rnd = race.get("round")
                    if rnd:
                        sprint_map[rnd] = [transform_result(r, is_qualy=False) for r in race.get("SprintResults", [])]

        # Map Sprint Qualifying (Future-proofing for when API supports it)
        sprint_qual_path = os.path.join(DATA_DIR, "sprint_qualifying.json")
        sprint_qual_map = {}
        if os.path.exists(sprint_qual_path):
            with open(sprint_qual_path, "r") as f:
                sq_data = json.load(f)
                for race in sq_data["MRData"]["RaceTable"]["Races"]:
                    # API might use SprintQualifyingResults or QualifyingResults internally for this endpoint
                    results = race.get("SprintQualifyingResults", race.get("QualifyingResults", []))
                    rnd = race.get("round")
                    if rnd:
                        sprint_qual_map[rnd] = [transform_result(r, is_qualy=True) for r in results]

        # Enrich schedule
        for race in races:
            rnd = race.get("round")
            if not rnd:
                continue  # Skip races without round number (unconfirmed rounds from API)
            if rnd in results_map:
                race["Results"] = results_map[rnd]
            if rnd in qual_map:
                race["QualifyingResults"] = qual_map[rnd]
            if rnd in sprint_map:
                race["SprintResults"] = sprint_map[rnd]
            if rnd in sprint_qual_map:
                race["SprintQualifyingResults"] = sprint_qual_map[rnd]

        with open(schedule_path, "w") as f:
            json.dump(sched_data, f, indent=2)
        print("  Successfully merged all results into current_schedule.json")
        
    except Exception as e:
        print(f"  Error during merge: {e}")


def git_commit_and_push():
    try:
        # Detect current branch
        branch = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True).stdout.strip()
        if not branch:
            branch = "master" # Fallback

        # Check if there are changes
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not status.stdout.strip():
            print("No changes to commit.")
            return

        print(f"Changes detected on branch '{branch}'. Committing and pushing...")
        
        # Add changes
        subprocess.run(["git", "add", "."], check=True)
        
        # Commit
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(["git", "commit", "-m", f"Auto-update F1 data: {timestamp}"], check=True)

        # Force sync with remote to avoid conflicts
        try:
            print("Syncing with remote...")
            subprocess.run(["git", "fetch", GITHUB_REMOTE], check=True)
            # We want to keep our local commit but move it on top of the remote or just force it
            # For a data bot, if conflicts happen, we usually want to just push our new data
            # To be safe, we'll try to rebase or just push. Given the bot nature, force push is sometimes acceptable
            # but let's try a safer 'push' after a fetch first. 
            # If rejected, we'll force it since this is a data-only repo for this bot.
            subprocess.run(["git", "push", GITHUB_REMOTE, branch], check=True)
            print("Successfully pushed to GitHub.")
        except subprocess.CalledProcessError:
            print("Push failed, trying force push...")
            subprocess.run(["git", "push", "-f", GITHUB_REMOTE, branch], check=True)
            print("Successfully force pushed to GitHub.")
        
    except subprocess.CalledProcessError as e:
        print(f"Git operation failed: {e}")


def sanitize_current_schedule():
    """
    Remove races without a 'round' field from current_schedule.json.
    The Jolpi/Ergast API sometimes returns unconfirmed races (e.g. Bahrain, Saudi Arabia
    at end of 2026 season) without a round number. Swift's APIRace model requires round
    as a non-optional String, so any race missing it causes the entire JSON decode to fail,
    which breaks fetchCalendar() and cascades to make all standings unreachable.
    """
    path = os.path.join(DATA_DIR, "current_schedule.json")
    if not os.path.exists(path):
        return
    with open(path, "r") as f:
        data = json.load(f)
    races = data["MRData"]["RaceTable"]["Races"]
    before = len(races)
    filtered = [r for r in races if r.get("round")]
    after = len(filtered)
    if before != after:
        data["MRData"]["RaceTable"]["Races"] = filtered
        data["MRData"]["total"] = str(after)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"  sanitize_current_schedule: removed {before - after} race(s) without round ({before} -> {after})")
    else:
        print(f"  sanitize_current_schedule: all {after} races have a round number, nothing to remove")

def job():
    print(f"Starting job at {datetime.now()}")
    ensure_data_dir()
    
    # Update a timestamp file to force a commit even if data hasn't changed
    with open(os.path.join(DATA_DIR, "last_updated.txt"), "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    success_count = 0
    for name, url in ENDPOINTS.items():
        if fetch_and_save(name, url) is not None:
            success_count += 1
            
    if success_count > 0:
        # Remove races without 'round' before any further processing
        sanitize_current_schedule()
        # Generate driver-team mapping
        fetch_driver_teams()
        # Fetch Sprint Qualifying from OpenF1 (not available in Jolpi/Ergast)
        fetch_sprint_qualifying_from_openf1()
        # Merge all data into current_schedule.json for the app
        merge_results_into_schedule()
        git_commit_and_push()
    
    print("Job finished.")

def fetch_sprint_qualifying_from_openf1():
    """
    Fetches Sprint Qualifying (Sprint Shootout) results from the OpenF1 API.
    The Jolpi/Ergast API does not expose this endpoint, so OpenF1 is used as
    the sole source for SprintQualifyingResults.

    Strategy:
      1. Find all sprint_qualifying sessions for the current year via OpenF1.
      2. For each session, retrieve the final position of every driver
         (last position entry = final qualifying classification).
      3. Map OpenF1 driver_number → ergast driverId via permanentNumber in drivers.json.
      4. Match each session to a schedule round by comparing meeting_name → raceName.
      5. Save the result as sprint_qualifying.json in a structure compatible with
         merge_results_into_schedule().
    """
    print("Fetching Sprint Qualifying from OpenF1...")
    try:
        year = datetime.now().year

        # ── 1. Find sprint qualifying sessions ──────────────────────────────
        # OpenF1 identifies these sessions via session_name="Sprint Qualifying"
        # (session_type is generic "Qualifying" — not specific enough on its own)
        sessions_url = f"https://api.openf1.org/v1/sessions?session_name=Sprint%20Qualifying&year={year}"
        resp = requests.get(sessions_url, timeout=15)
        resp.raise_for_status()
        sessions = resp.json()

        if not sessions:
            print("  OpenF1: no Sprint Qualifying sessions found for current year.")
            return False

        print(f"  Found {len(sessions)} Sprint Qualifying session(s).")

        # ── 2. Build driver-number → ergast info map ─────────────────────────
        drivers_path = os.path.join(DATA_DIR, "drivers.json")
        if not os.path.exists(drivers_path):
            print("  drivers.json not found – cannot map driver IDs. Skipping.")
            return False

        with open(drivers_path, "r") as f:
            drivers_data = json.load(f)

        # Build full driver lookup by driverId AND by permanentNumber
        driverId_to_full_driver = {
            d["driverId"]: d
            for d in drivers_data["MRData"]["DriverTable"]["Drivers"]
        }
        number_to_ergast = {}
        for d in drivers_data["MRData"]["DriverTable"]["Drivers"]:
            num = str(d.get("permanentNumber", "")).strip()
            if num:
                number_to_ergast[num] = d  # Store full driver object (all fields)

        # Also index actual racing numbers from sprint.json:
        # the championship holder uses #1 instead of their permanent number,
        # which would otherwise be missed by the permanentNumber lookup above.
        sprint_path_tmp = os.path.join(DATA_DIR, "sprint.json")
        if os.path.exists(sprint_path_tmp):
            with open(sprint_path_tmp, "r") as f:
                sprint_tmp = json.load(f)
            for race in sprint_tmp["MRData"]["RaceTable"]["Races"]:
                for result in race.get("SprintResults", []):
                    num = str(result.get("number", "")).strip()
                    drv = result.get("Driver", {})
                    did = drv.get("driverId", "")
                    if num and did and num not in number_to_ergast:
                        # Prefer full driver from drivers.json; fallback to sprint data
                        number_to_ergast[num] = driverId_to_full_driver.get(did, drv)

        # ── 3. Build driverId → constructorId map ────────────────────────────
        # Build full constructor lookup
        constructors_path = os.path.join(DATA_DIR, "constructors.json")
        constructorId_to_full = {}
        if os.path.exists(constructors_path):
            with open(constructors_path, "r") as f:
                ctor_data = json.load(f)
                for c in ctor_data["MRData"]["ConstructorTable"]["Constructors"]:
                    constructorId_to_full[c["constructorId"]] = c

        driver_to_constructor = {}
        dt_path = os.path.join(DATA_DIR, "driver_teams.json")
        if os.path.exists(dt_path):
            with open(dt_path, "r") as f:
                for entry in json.load(f):
                    did = entry["driver"]["driverId"]
                    cid = entry["constructor"]["constructorId"]
                    # Use full constructor from constructors.json; fallback to driver_teams data
                    driver_to_constructor[did] = constructorId_to_full.get(cid, entry["constructor"])

        # ── 4. Build lookup maps from current schedule ───────────────────────
        # OpenF1 sessions have no meeting_name; we match on:
        #   session.location (e.g. "Shanghai") → race.Circuit.Location.locality
        #   session.country_name (e.g. "China") → race.Circuit.Location.country  (fallback)
        schedule_path = os.path.join(DATA_DIR, "current_schedule.json")
        if not os.path.exists(schedule_path):
            print("  current_schedule.json not found – cannot match rounds. Skipping.")
            return False

        with open(schedule_path, "r") as f:
            sched_data = json.load(f)

        races = sched_data["MRData"]["RaceTable"]["Races"]
        # Build locality and country lookup (lowercased for comparison)
        locality_list = [
            (race["Circuit"]["Location"]["locality"].lower(), race.get("round"))
            for race in races if race.get("round")  # skip unconfirmed rounds
        ]
        country_list = [
            (race["Circuit"]["Location"]["country"].lower(), race.get("round"))
            for race in races if race.get("round")  # skip unconfirmed rounds
        ]

        def match_location(location, country_name):
            """Exact then partial match on locality, then country."""
            loc_l = location.lower()
            cnt_l = country_name.lower()
            # 1. Exact locality match
            for locality, rnd in locality_list:
                if loc_l == locality:
                    return rnd
            # 2. Partial locality match (handles "Miami Gardens" ↔ "Miami" etc.)
            for locality, rnd in locality_list:
                if loc_l in locality or locality in loc_l:
                    return rnd
            # 3. Exact country match
            for country, rnd in country_list:
                if cnt_l == country:
                    return rnd
            return None

        # ── 5. Process each session ──────────────────────────────────────────
        sq_results_by_round = {}

        for session in sessions:
            session_key  = session.get("session_key")
            location     = (session.get("location") or "").strip()       # e.g. "Shanghai"
            country_name = (session.get("country_name") or "").strip()   # e.g. "China"
            print(f"  Processing session {session_key} – {location} ({country_name})")

            # Match to a schedule round
            round_num = match_location(location, country_name)
            if not round_num:
                print(f"    Could not match location='{location}' / country='{country_name}' to a round. Skipping.")
                continue

            # Fetch all position entries for this session
            # Sessions that haven't happened yet return 404 → skip gracefully
            try:
                pos_url = f"https://api.openf1.org/v1/position?session_key={session_key}"
                pos_resp = requests.get(pos_url, timeout=20)
                pos_resp.raise_for_status()
                positions = pos_resp.json()
            except Exception as pos_err:
                print(f"    Position data not available for session {session_key} ({pos_err}). Skipping.")
                continue

            if not positions:
                print(f"    No position data for session {session_key}. Skipping.")
                continue

            # Keep only the LAST position entry per driver (= final classification)
            final_pos = {}   # driver_number (str) → position (int)
            for entry in positions:
                drv_num = str(entry.get("driver_number", ""))
                pos_val = entry.get("position")
                if drv_num and pos_val is not None:
                    final_pos[drv_num] = int(pos_val)

            # Sort drivers by their final position
            sorted_drivers = sorted(final_pos.items(), key=lambda x: x[1])

            results = []
            for drv_num, pos_val in sorted_drivers:
                ergast_drv = number_to_ergast.get(drv_num)
                if not ergast_drv:
                    print(f"    Warning: driver #{drv_num} not found in drivers.json – skipped.")
                    continue

                driver_id   = ergast_drv["driverId"]
                constructor = driver_to_constructor.get(driver_id, {"constructorId": ""})

                results.append({
                    "number":       drv_num,
                    "position":     str(pos_val),
                    "positionText": str(pos_val),
                    "points":       "0",
                    "grid":         "",
                    "laps":         "",
                    "status":       "Finished",
                    "Driver":       ergast_drv,
                    "Constructor":  constructor
                })

            if results:
                sq_results_by_round[round_num] = results
                print(f"    Round {round_num}: {len(results)} driver(s) classified.")

            time.sleep(0.5)   # be polite to OpenF1

        if not sq_results_by_round:
            print("  No sprint qualifying results to save.")
            return False

        # ── 6. Save sprint_qualifying.json ───────────────────────────────────
        output = {
            "MRData": {
                "RaceTable": {
                    "Races": [
                        {"round": rnd, "SprintQualifyingResults": res}
                        for rnd, res in sq_results_by_round.items()
                    ]
                }
            }
        }
        sq_path = os.path.join(DATA_DIR, "sprint_qualifying.json")
        with open(sq_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Saved sprint_qualifying.json ({len(sq_results_by_round)} round(s)).")
        return True

    except Exception as e:
        print(f"  Error fetching sprint qualifying from OpenF1: {e}")
        return False


def fetch_driver_teams():
    print("Fetching driver-team mappings...")
    try:
        # Get constructors first
        url = ENDPOINTS["constructors"]
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        constructors_data = response.json()
        constructors = constructors_data["MRData"]["ConstructorTable"]["Constructors"]
        
        driver_teams = []
        
        for constructor in constructors:
            c_id = constructor["constructorId"]
            # Fetch drivers for this constructor
            d_url = f"http://api.jolpi.ca/ergast/f1/current/constructors/{c_id}/drivers.json"
            print(f"  Fetching drivers for {c_id}...")
            d_response = requests.get(d_url, timeout=10)
            d_response.raise_for_status()
            d_data = d_response.json()
            drivers = d_data["MRData"]["DriverTable"]["Drivers"]
            
            for driver in drivers:
                entry = {
                    "driver": driver,
                    "constructor": constructor
                }
                driver_teams.append(entry)
            
            # Be nice to the API
            time.sleep(0.2)
            
        file_path = os.path.join(DATA_DIR, "driver_teams.json")
        with open(file_path, "w") as f:
            json.dump(driver_teams, f, indent=2)
        print(f"Saved driver_teams.json with {len(driver_teams)} entries.")
        return True

    except Exception as e:
        print(f"Error generating driver teams: {e}")
        return False


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
