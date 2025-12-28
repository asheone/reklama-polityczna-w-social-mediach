# Meta Ad Library Collector

This module collects political and issue ads from the Meta (Facebook) Ad Library API.

## Setup

### 1. Create a Meta App

1. Go to [Meta for Developers](https://developers.facebook.com/)
2. Create a new app (choose "Business" type)
3. Add the "Marketing API" product to your app

### 2. Get an Access Token

There are two ways to get an access token:

#### Option A: Graph API Explorer (Quick testing)
1. Go to [Graph API Explorer](https://developers.facebook.com/tools/explorer/)
2. Select your app
3. Click "Generate Access Token"
4. Grant required permissions: `ads_read`

#### Option B: Long-Lived Token (Production)
1. Get a short-lived token from Graph API Explorer
2. Exchange it for a long-lived token (60 days):
   ```bash
   curl "https://graph.facebook.com/v19.0/oauth/access_token?grant_type=fb_exchange_token&client_id=YOUR_APP_ID&client_secret=YOUR_APP_SECRET&fb_exchange_token=SHORT_LIVED_TOKEN"
   ```

### 3. Configure Environment

Add your token to `.env`:
```bash
META_ACCESS_TOKEN=your_token_here
```

### 4. Test Credentials

```bash
python scripts/test_credentials.py --platform meta
```

## Usage

### Basic Collection

```bash
# Fetch one week of Polish political ads
python scripts/run_meta_collector.py \
    --start-date 2024-12-01 \
    --end-date 2024-12-07
```

### Full Year Collection

```bash
# Fetch entire year (use resume for long-running jobs)
python scripts/run_meta_collector.py \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --resume
```

### Dry Run (Testing)

```bash
# Test without writing output
python scripts/run_meta_collector.py \
    --start-date 2024-12-01 \
    --end-date 2024-12-07 \
    --dry-run
```

## Output Format

Data is written as NDJSON (newline-delimited JSON) files:

```
output/meta/
├── meta_ads_20241215_143052_batch_0001.ndjson
├── meta_ads_20241215_143052_batch_0002.ndjson
└── meta_ads_20241215_143052_manifest.json
```

### Record Schema

Each ad record contains:

| Field | Type | Description |
|-------|------|-------------|
| `ad_id` | string | Unique ad identifier |
| `platform` | string | Always "meta" |
| `advertiser_name` | string | Page name |
| `page_id` | string | Page ID |
| `funding_entity` | string | Who paid for the ad |
| `start_date` | string | ISO format delivery start |
| `end_date` | string | ISO format delivery end |
| `spend_min` | float | Minimum spend |
| `spend_max` | float | Maximum spend |
| `spend_currency` | string | Currency code (PLN) |
| `impressions_min` | int | Minimum impressions |
| `impressions_max` | int | Maximum impressions |
| `ad_content` | string | Ad text/caption |
| `targeting_data` | object | Demographics, regions, etc. |
| `ad_snapshot_url` | string | URL to ad preview |
| `raw_response` | object | Full API response |

## Rate Limits

Meta Ad Library API limits:
- ~200 requests per minute
- Each request returns up to 500 ads
- Long-running queries may timeout

The collector handles rate limiting automatically with exponential backoff.

## Troubleshooting

### "Invalid access token"
- Token may have expired (they last 60 days)
- Generate a new token and update `.env`

### "Rate limit exceeded"
- The collector automatically waits and retries
- If persistent, reduce `requests_per_minute` in config

### "Permission denied"
- Ensure your app has `ads_read` permission
- Check that your app is approved for Marketing API access

## API Reference

- [Ad Library API Documentation](https://www.facebook.com/ads/library/api/)
- [Marketing API Reference](https://developers.facebook.com/docs/marketing-api/)
- [Graph API Explorer](https://developers.facebook.com/tools/explorer/)
