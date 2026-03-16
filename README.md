# Shiny for Python Last.fm App

A **Shiny for Python** dashboard that connects to the [Last.fm API](https://www.last.fm/api) via [pylast](https://pypi.org/project/pylast/) to explore global music trends.

**Live app:** https://tiagoadrianunes.shinyapps.io/lastfm-global-trends/

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- A [Last.fm API account](https://www.last.fm/api/account/create)

## Setup

1. Clone the repository and install dependencies:

   ```bash
   git clone https://github.com/your-username/lastfm-global-trends.git
   cd lastfm-global-trends
   uv sync
   ```

2. Get a Last.fm API key:

   - Log in or sign up at [last.fm](https://www.last.fm)
   - Go to [last.fm/api/account/create](https://www.last.fm/api/account/create)
   - Fill in the application name and description, then submit
   - Copy the **API key** and **Shared secret** from the confirmation page

3. Create a `.env` file with your Last.fm credentials:

   ```env
   LASTFM_API_KEY=your_api_key
   LASTFM_API_SECRET=your_api_secret
   ```

4. Run the app:

   ```bash
   uv run shiny run --reload app.py
   ```

5. Open your browser at `http://127.0.0.1:8000`

## Deployment

### Generating requirements.txt

Before deploying, generate the `requirements.txt` file:

```bash
uv export --no-dev --no-hashes --no-annotate -o requirements.txt
```

### Deploying to shinyapps.io

1. Install rsconnect-python:

   ```bash
   uv add --dev rsconnect-python
   ```

2. Get your credentials at [shinyapps.io](https://www.shinyapps.io) → Account → Tokens → Show → Show secret

3. Configure the account (one-time setup):

   ```bash
   rsconnect add \
     --account YOUR_ACCOUNT_NAME \
     --name YOUR_ACCOUNT_NAME \
     --token YOUR_TOKEN \
     --secret YOUR_SECRET
   ```

4. Deploy:

   ```bash
   rsconnect deploy shiny . \
     --name YOUR_ACCOUNT_NAME \
     --title lastfm-global-trends
   ```

   > **Redeploying an existing app:** The free tier allows only 5 apps. If you hit the limit, target the existing app by ID to update it in place instead of creating a new one:
   >
   > ```bash
   > rsconnect deploy shiny . \
   >   --name YOUR_ACCOUNT_NAME \
   >   --title lastfm-global-trends \
   >   --app-id 16891361
   > ```
   >
   > To find the app ID: `rsconnect apps list --name YOUR_ACCOUNT_NAME`

## Data Fetching

The app reads from a local DuckDB database (`data/trends.db`). The `fetch_countries.py` script populates it by pulling top artists and tracks from the Last.fm API for every supported country plus global charts.

### Basic usage

```bash
uv run python fetch_countries.py
```

Fetches all countries and global charts. Data already in the DB is skipped if it was fetched within the last **7 days** (default) and the row count looks complete.

Logs are written to `data/logs/run_<timestamp>.log` and a JSON summary to `data/logs/summary_<timestamp>.json`.

### Options

#### `--max-age HOURS`

Controls how old data must be before it is re-fetched. Defaults to `168` (7 days).

```bash
# Re-fetch anything older than 24 hours
uv run python fetch_countries.py --max-age 24

# Weekly scheduled run (explicit)
uv run python fetch_countries.py --max-age 168
```

> **Note:** Even within the max-age window, a country is always re-fetched if the number of pages returned by the API no longer matches what is stored in the DB (e.g. new artists entered the chart).

#### `--max-age 0` — Force full refresh

Set `--max-age 0` to force a complete re-fetch of everything regardless of when it was last imported:

```bash
uv run python fetch_countries.py --max-age 0
```

#### `--only` — Fetch specific countries

Pass a comma-separated list of country display names to limit the run. Global charts are skipped when `--only` is used.

```bash
# Single country
uv run python fetch_countries.py --only "France"

# Multiple countries
uv run python fetch_countries.py --only "France,Germany,Japan"

# Force refresh for specific countries only
uv run python fetch_countries.py --only "France,Germany" --max-age 0
```

Country names must match the display names in the app (e.g. `"Côte d'Ivoire"`, `"Türkiye"`, `"Czechia"`). A full list can be found in `countries.py`.

### How the skip logic works

On each run, for every country the script:

1. Checks `fetched_at` in the DB — if older than `--max-age`, marks it **stale**
2. Fetches page 1 from the API (always, to get the current page count)
3. Compares the API page count against the DB row count:
   - **Fresh + count matches** → skip entirely, no write
   - **Fresh + count mismatch** → re-fetch all pages and replace DB rows
   - **Stale** → re-fetch all pages and replace DB rows unconditionally

When rows are written, the script verifies the saved count matches the fetched count and logs a warning if they differ.

### Scheduling (cron example)

```cron
# Run every Sunday at 2 AM
0 2 * * 0 cd /path/to/lastfm-global-trends && uv run python fetch_countries.py >> data/logs/cron.log 2>&1
```
