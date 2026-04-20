# bal-dashboard

Static site for comparing BAL benchmark results across execution clients (besu, geth, nethermind) and modes (sequential, nobatchio, full).

Data source: [benchmarkoor-api](https://benchmarkoor-api.core.ethpandaops.io).

## Usage

Open the deployed site and paste your Benchmarkoor API key when prompted. The key is stored in `localStorage` and is never committed to the repo or visible in its source.

## Local dev

```
python3 -m http.server 8000
```

Open `http://localhost:8000/`.

## Deployment

Pushes to `main` trigger `.github/workflows/deploy.yml`, which publishes the repository contents to GitHub Pages. Enable Pages in repo settings with **Source: GitHub Actions**.
