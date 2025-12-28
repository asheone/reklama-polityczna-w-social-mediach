# Political Advertising Data Collector

Modular framework for collecting Polish political advertising data from multiple platforms (Meta, Google, TikTok).

## Features

- **Modular Architecture**: Easy to add new data sources by implementing `BaseAdCollector`
- **Cloud-Ready**: Works locally for prototyping, deploys to GCP Cloud Functions for production
- **Resumable**: Checkpoint system allows resuming long-running collections
- **Rate Limited**: Token bucket algorithm prevents API throttling
- **Standardized Output**: All platforms output to same NDJSON schema

## Supported Platforms

| Platform | Status | Data Source |
|----------|--------|-------------|
| Meta (Facebook/Instagram) | ✅ Ready | Meta Ad Library API |
| Google Ads | 🔧 Stub | BigQuery Public Dataset |
| TikTok | 🔧 Stub | Manual CSV Import |

## Quick Start

### 1. Clone and Install

```bash
git clone <repo-url>
cd political-ad-collector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Credentials

```bash
# Copy example environment file
cp .env.example .env

# Edit .env and add your Meta API token
# Get token from: https://developers.facebook.com/tools/explorer/
```

### 3. Test API Access

```bash
python scripts/test_credentials.py
```

### 4. Run Meta Collector

```bash
# Fetch one week of Polish political ads
python scripts/run_meta_collector.py \
    --start-date 2024-12-01 \
    --end-date 2024-12-07

# Fetch full year (use --resume for long runs)
python scripts/run_meta_collector.py \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --resume
```

### 5. Output

Data is written to `output/meta/`:

```
output/meta/
├── meta_ads_20241215_143052_batch_0001.ndjson
├── meta_ads_20241215_143052_batch_0002.ndjson
└── meta_ads_20241215_143052_manifest.json
```

## Project Structure

```
political-ad-collector/
├── collectors/                 # Data source implementations
│   ├── base.py                # Abstract base class
│   ├── meta/                  # Meta Ad Library collector
│   │   ├── collector.py       # MetaAdCollector class
│   │   └── config.yaml        # Meta-specific config
│   ├── google/                # Google Ads (stub)
│   └── tiktok/                # TikTok (stub)
├── shared/                    # Shared utilities
│   ├── rate_limiter.py       # Token bucket rate limiter
│   ├── checkpoint_manager.py # Progress checkpointing
│   ├── output_writer.py      # NDJSON output
│   ├── logger.py             # Structured logging
│   └── exceptions.py         # Custom exceptions
├── cloud_functions/           # GCP deployment
│   ├── meta_daily_sync/      # Cloud Function code
│   └── deploy.sh             # Deployment script
├── scripts/                   # CLI tools
│   ├── run_meta_collector.py # Main collector runner
│   ├── test_credentials.py   # Credential testing
│   └── upload_to_bigquery.py # BigQuery upload
├── config/                    # Configuration files
├── tests/                     # Unit tests
└── output/                    # Data output (gitignored)
```

## Standard Output Schema

All collectors output data in the same schema:

| Field | Type | Description |
|-------|------|-------------|
| `ad_id` | string | Unique ad identifier |
| `platform` | string | Platform name (meta/google/tiktok) |
| `advertiser_name` | string | Advertiser/page name |
| `start_date` | string | Ad start date (ISO format) |
| `end_date` | string | Ad end date (ISO format) |
| `spend_min` | float | Minimum spend amount |
| `spend_max` | float | Maximum spend amount |
| `spend_currency` | string | Currency code |
| `impressions_min` | int | Minimum impressions |
| `impressions_max` | int | Maximum impressions |
| `ad_content` | string | Ad text/caption |
| `targeting_data` | object | Targeting information |
| `raw_response` | object | Full API response |
| `extracted_at` | string | Extraction timestamp |

## Adding New Data Sources

1. Create a new collector in `collectors/new_platform/`:

```python
from collectors.base import BaseAdCollector

class NewPlatformCollector(BaseAdCollector):
    @property
    def platform_name(self) -> str:
        return "new_platform"

    def authenticate(self) -> bool:
        # Verify API credentials
        pass

    def fetch_ads(self, start_date, end_date, country_code):
        # Yield raw API responses
        pass

    def transform_ad(self, raw_ad):
        # Transform to standard schema
        pass

    def validate_record(self, record):
        # Validate required fields
        pass
```

2. Add configuration in `collectors/new_platform/config.yaml`
3. Create runner script in `scripts/run_new_platform_collector.py`

## Cloud Deployment

### Prerequisites

- GCP project with billing enabled
- Required APIs: Cloud Functions, Cloud Storage, Secret Manager, Cloud Scheduler

### Deploy

```bash
# Set up credentials
echo -n "your_meta_token" | gcloud secrets create META_ACCESS_TOKEN --data-file=-

# Deploy
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET_NAME=polish-political-ads
./cloud_functions/deploy.sh
```

This creates:
- Cloud Function `meta-daily-sync`
- Cloud Storage bucket for output
- Cloud Scheduler job (daily at 02:00 UTC)

## Development

### Setup

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Install package in editable mode
pip install -e .
```

### Testing

```bash
# Run tests
pytest

# With coverage
pytest --cov=collectors --cov=shared
```

### Code Quality

```bash
# Format code
black .
isort .

# Type checking
mypy collectors/ shared/

# Linting
flake8
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `META_ACCESS_TOKEN` | Meta API access token | Yes (for Meta) |
| `GCP_PROJECT_ID` | GCP project ID | For cloud mode |
| `GCS_BUCKET_NAME` | Cloud Storage bucket | For cloud mode |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) | No |

### Rate Limiting

Default Meta API limits:
- 180 requests per minute
- 500 ads per request
- ~90,000 ads per minute maximum

The collector automatically handles rate limiting with exponential backoff.

## Troubleshooting

### "Invalid access token"
- Token may have expired (60 day lifetime)
- Generate new token at [Graph API Explorer](https://developers.facebook.com/tools/explorer/)

### "Rate limit exceeded"
- Collector will automatically wait and retry
- Reduce `requests_per_minute` in config if persistent

### "Checkpoint exists"
- Use `--resume` to continue from checkpoint
- Use `--clear-checkpoint` to start fresh

## License

MIT License - See LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and add tests
4. Run `pytest` and `black .`
5. Submit a pull request
