# Meta Daily Sync Cloud Function

GCP Cloud Function that fetches Meta political ads daily and stores them in Cloud Storage.

## Overview

This Cloud Function:
1. Runs daily at 02:00 UTC via Cloud Scheduler
2. Fetches political ads from Meta Ad Library API for the previous day
3. Writes NDJSON output to Cloud Storage
4. Stores checkpoints for resumability

## Deployment

### Prerequisites

1. GCP project with billing enabled
2. Required APIs enabled:
   - Cloud Functions
   - Cloud Storage
   - Cloud Scheduler
   - Secret Manager

3. Service account with roles:
   - Cloud Functions Invoker
   - Storage Object Admin
   - Secret Manager Secret Accessor

### Setup Secrets

Store Meta access token in Secret Manager:

```bash
echo -n "your_access_token" | gcloud secrets create META_ACCESS_TOKEN --data-file=-
```

### Deploy

```bash
./deploy.sh
```

Or manually:

```bash
gcloud functions deploy meta-daily-sync \
    --gen2 \
    --runtime python311 \
    --region europe-west1 \
    --source . \
    --entry-point meta_daily_sync \
    --trigger-http \
    --timeout 540s \
    --memory 512MB \
    --set-env-vars GCS_BUCKET_NAME=polish-political-ads,GCP_PROJECT_ID=your-project
```

### Create Scheduler

```bash
gcloud scheduler jobs create http meta-daily-sync-job \
    --location europe-west1 \
    --schedule "0 2 * * *" \
    --uri "https://europe-west1-YOUR_PROJECT.cloudfunctions.net/meta-daily-sync" \
    --http-method POST \
    --oidc-service-account-email meta-collector@YOUR_PROJECT.iam.gserviceaccount.com \
    --time-zone "UTC"
```

## Manual Trigger

### Daily sync (yesterday's data)
```bash
curl -X POST https://REGION-PROJECT.cloudfunctions.net/meta-daily-sync \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json"
```

### Specific date
```bash
curl -X POST https://REGION-PROJECT.cloudfunctions.net/meta-daily-sync \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"date": "2024-12-15"}'
```

### Backfill
```bash
curl -X POST https://REGION-PROJECT.cloudfunctions.net/meta-backfill \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'
```

## Output

Data is written to Cloud Storage:

```
gs://polish-political-ads/
├── meta/
│   ├── meta_ads_20241215_020000_batch_0001.ndjson
│   ├── meta_ads_20241215_020000_manifest.json
│   └── ...
└── checkpoints/
    └── meta_checkpoint.json
```

## Monitoring

View logs:
```bash
gcloud functions logs read meta-daily-sync --region europe-west1
```

## Costs

Estimated monthly costs:
- Cloud Functions: ~$5 (daily 5-10 minute runs)
- Cloud Storage: ~$1-5 (depends on data volume)
- Cloud Scheduler: <$1
- Secret Manager: <$1
