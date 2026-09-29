# Shiny for Python Last.fm App

A **Shiny for Python** dashboard that visualises global Last.fm music trends — top artists, tracks, tags, and country breakdowns.

**Live app:** https://tiagoadrianunes.shinyapps.io/lastfm-global-trends/

**Dataset:** https://www.kaggle.com/datasets/tiagoadrianunes/last-fm-global-trends

## How it works

The app reads from a pre-built **DuckDB** database (`data/trends.db`) hosted on Kaggle. On first run it downloads the database automatically — no live Last.fm API calls at runtime.

The `fetch_countries.py` script is used separately to rebuild the database from the Last.fm API and upload a new version to Kaggle when a refresh is needed.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- A [Kaggle account](https://www.kaggle.com) with API credentials

## Setup

1. Clone the repository and install dependencies:

   ```bash
   git clone https://github.com/TiagoAdriaNunes/lastfm-global-trends.git
   cd lastfm-global-trends
   uv sync
   ```

2. Get your Kaggle API credentials:

   - Go to https://www.kaggle.com/settings → API → **Create New Token**
   - This downloads a `kaggle.json` with your `username` and `key`

3. Create a `.env` file with your Kaggle credentials:

   ```env
   KAGGLE_USERNAME=your_username
   KAGGLE_KEY=your_api_key
   ```

4. Run the app (`data/trends.db` is downloaded automatically on first run):

   ```bash
   uv run shiny run --reload app.py
   ```

5. Open your browser at `http://127.0.0.1:8000`

## Deployment

### Deploying to shinyapps.io

1. Install rsconnect-python:

   ```bash
   uv add --dev rsconnect-python
   ```

2. Get your rsconnect token at [shinyapps.io](https://www.shinyapps.io) → Account → Tokens:

   - Click **Show** next to your token (or **Add Token** if you don't have one yet).
   - In the dialog, open the **With Python** tab and click **Copy to clipboard**. This copies a ready-to-run `rsconnect add ...` command with your account name, token and secret filled in.
   - Alternatively, click **Show secret** and copy the token and secret yourself.

3. Configure the account (one-time setup) by pasting the copied command into your terminal (prefix it with `uv run` if the project's virtual environment isn't activated), or by running it with your own values:

   ```bash
   uv run rsconnect add \
     --account YOUR_ACCOUNT_NAME \
     --name YOUR_ACCOUNT_NAME \
     --token YOUR_TOKEN \
     --secret YOUR_SECRET
   ```

   The credentials are saved locally by rsconnect-python, so you don't need to run this again on the same machine. Keep the token and secret out of the repository.

4. Add the deploy settings to your `.env`:

   ```env
   SHINYAPPS_ACCOUNT=your_account_name   # same name used in `rsconnect add --name`
   SHINYAPPS_APP_ID=your_app_id          # ID of the existing app to update
   KAGGLE_API_TOKEN=your_kaggle_token    # used by the deployed app to download trends.db
   ```

   To find the app ID: `uv run rsconnect apps list --name YOUR_ACCOUNT_NAME`.

5. Deploy with the script:

   ```bash
   # Check which files would be uploaded (nothing is deployed)
   uv run python scripts/deploy.py --dry-run

   # Deploy, updating the existing app in place
   uv run python scripts/deploy.py
   ```

   The script reads `SHINYAPPS_ACCOUNT` and `SHINYAPPS_APP_ID` from `.env` (or from `--account` / `--app-id`) and refuses to deploy if either is missing, so it never creates a new app by accident (the free tier allows only 5 apps). It generates `requirements.txt` from `uv.lock`, excludes the local `data/` folder and files the app doesn't need, and cleans up the files it generated.

   > **Note:** `.env` is uploaded with the app on purpose. shinyapps.io doesn't receive environment variables from rsconnect, so the deployed app reads `KAGGLE_API_TOKEN` from the uploaded `.env` to download `trends.db`. Everything else in `.env` is uploaded too, so keep it limited to what you're happy to store on shinyapps.io.

## Rebuilding the database

`fetch_countries.py` pulls top artists, tracks, and tags from the Last.fm API for every supported country plus global charts, and writes the results into `data/trends.db`. You only need this to refresh the data and publish a new Kaggle dataset version.

### Requirements

- A [Last.fm API key](https://www.last.fm/api/account/create)
- Add to your `.env`:
  ```env
  LASTFM_API_KEY=your_api_key
  LASTFM_API_SECRET=your_api_secret
  ```

### Basic usage

```bash
uv run python fetch_countries.py
```

Fetches all countries and global charts. Country charts are limited to the top **1,000** artists and tracks per country. Data already in the DB is skipped if it was fetched within the last **7 days** (default) and the row count looks complete.

Logs are written to `data/logs/run_<timestamp>.log` and a JSON summary to `data/logs/summary_<timestamp>.json`.

### Options

#### `--max-age HOURS`

Controls how old data must be before it is re-fetched. Defaults to `168` (7 days).

```bash
# Re-fetch anything older than 24 hours
uv run python fetch_countries.py --max-age 24

# Force full refresh of everything
uv run python fetch_countries.py --max-age 0
```

> **Note:** Even within the max-age window, a country is always re-fetched if the number of pages returned by the API no longer matches what is stored in the DB.

#### `--max-rank N`

Keeps only the top N artists and tracks per country. Defaults to `1000`. Rows beyond this rank are deleted from the DB at the start of each run.

```bash
uv run python fetch_countries.py --max-rank 500
```

> **Note:** The [Last.fm API Terms of Service](https://www.last.fm/api/tos) (clause 4.3.4) cap stored Last.fm data at **100 MB in total**, including anything published. Raising `--max-rank` increases the DB size, so keep the total under that limit.

#### `--only` — Fetch specific countries

```bash
# Single country
uv run python fetch_countries.py --only "France"

# Multiple countries (global charts are skipped)
uv run python fetch_countries.py --only "France,Germany,Japan"
```

Country names must match the display names in the app (e.g. `"Côte d'Ivoire"`, `"Türkiye"`). A full list is in `countries.py`.

### How the skip logic works

On each run, for every country the script:

1. Checks `fetched_at` in the DB — if older than `--max-age`, marks it **stale**
2. Fetches page 1 from the API (always, to get the current page count)
3. Compares the API page count (capped at `--max-rank`) against the DB row count:
   - **Fresh + count matches** → skip entirely, no write
   - **Fresh + count mismatch** → re-fetch up to `--max-rank` rows and replace DB rows
   - **Stale** → re-fetch up to `--max-rank` rows and replace DB rows unconditionally

### Scheduling (cron example)

```cron
# Run every Sunday at 2 AM
0 2 * * 0 cd /path/to/lastfm-global-trends && uv run python fetch_countries.py >> data/logs/cron.log 2>&1
```
