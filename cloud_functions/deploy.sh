#!/bin/bash
# Deploy Meta daily sync Cloud Function

set -e

# Configuration (update these for your project)
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
REGION="${GCP_REGION:-europe-west1}"
FUNCTION_NAME="meta-daily-sync"
BUCKET_NAME="${GCS_BUCKET_NAME:-polish-political-ads}"
SERVICE_ACCOUNT="meta-collector@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=========================================="
echo "Deploying Meta Daily Sync Cloud Function"
echo "=========================================="
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET_NAME}"
echo "=========================================="

# Check if gcloud is configured
if ! gcloud config get-value project &>/dev/null; then
    echo "Error: gcloud not configured. Run 'gcloud init' first."
    exit 1
fi

# Enable required APIs
echo "Enabling required APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    storage.googleapis.com \
    --project="${PROJECT_ID}"

# Create bucket if doesn't exist
echo "Creating Cloud Storage bucket..."
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${BUCKET_NAME}" 2>/dev/null || true

# Create service account if doesn't exist
echo "Creating service account..."
gcloud iam service-accounts create meta-collector \
    --display-name="Meta Ads Collector" \
    --project="${PROJECT_ID}" 2>/dev/null || true

# Grant permissions
echo "Granting permissions..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/storage.objectAdmin" \
    --quiet

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet

# Check if secret exists
echo "Checking META_ACCESS_TOKEN secret..."
if ! gcloud secrets describe META_ACCESS_TOKEN --project="${PROJECT_ID}" &>/dev/null; then
    echo ""
    echo "WARNING: META_ACCESS_TOKEN secret not found!"
    echo "Create it with:"
    echo "  echo -n 'your_token' | gcloud secrets create META_ACCESS_TOKEN --data-file=-"
    echo ""
    read -p "Press Enter to continue anyway, or Ctrl+C to abort..."
fi

# Deploy function
echo "Deploying Cloud Function..."
gcloud functions deploy "${FUNCTION_NAME}" \
    --gen2 \
    --runtime python311 \
    --region "${REGION}" \
    --source ./cloud_functions/meta_daily_sync \
    --entry-point meta_daily_sync \
    --trigger-http \
    --no-allow-unauthenticated \
    --timeout 540s \
    --memory 512MB \
    --service-account "${SERVICE_ACCOUNT}" \
    --set-env-vars "GCS_BUCKET_NAME=${BUCKET_NAME},GCP_PROJECT_ID=${PROJECT_ID}" \
    --project="${PROJECT_ID}"

echo ""
echo "Function deployed successfully!"

# Get function URL
FUNCTION_URL=$(gcloud functions describe "${FUNCTION_NAME}" \
    --region "${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(serviceConfig.uri)")

echo "Function URL: ${FUNCTION_URL}"

# Create Cloud Scheduler job
echo ""
echo "Creating Cloud Scheduler job..."
gcloud scheduler jobs create http meta-daily-sync-job \
    --location "${REGION}" \
    --schedule "0 2 * * *" \
    --uri "${FUNCTION_URL}" \
    --http-method POST \
    --oidc-service-account-email "${SERVICE_ACCOUNT}" \
    --time-zone "UTC" \
    --project="${PROJECT_ID}" 2>/dev/null || \
    gcloud scheduler jobs update http meta-daily-sync-job \
        --location "${REGION}" \
        --schedule "0 2 * * *" \
        --uri "${FUNCTION_URL}" \
        --http-method POST \
        --oidc-service-account-email "${SERVICE_ACCOUNT}" \
        --time-zone "UTC" \
        --project="${PROJECT_ID}"

echo ""
echo "=========================================="
echo "Deployment complete!"
echo "=========================================="
echo ""
echo "Cloud Function: ${FUNCTION_URL}"
echo "Scheduler: Daily at 02:00 UTC"
echo "Output bucket: gs://${BUCKET_NAME}/meta/"
echo ""
echo "To test manually:"
echo "  curl -X POST ${FUNCTION_URL} -H 'Authorization: Bearer \$(gcloud auth print-identity-token)'"
echo ""
