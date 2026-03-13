# lastfm-global-trends

A **Shiny for Python** dashboard that connects to the [Last.fm API](https://www.last.fm/api) via [pylast](https://pypi.org/project/pylast/) to explore global music trends.

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

