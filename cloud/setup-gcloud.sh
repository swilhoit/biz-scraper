#!/bin/bash

# Business Listings Scraper - Complete GCloud Setup Script
# This script sets up everything needed for the cloud scraper using gcloud CLI

set -e

echo "==================================="
echo "Business Scraper GCloud Setup"
echo "==================================="

# Check if required environment variables are set
if [ -z "$GCP_PROJECT_ID" ]; then
    read -p "Enter your GCP Project ID: " GCP_PROJECT_ID
    export GCP_PROJECT_ID=$GCP_PROJECT_ID
fi

if [ -z "$SCRAPER_API_KEY" ]; then
    read -p "Enter your ScraperAPI Key: " SCRAPER_API_KEY
    export SCRAPER_API_KEY=$SCRAPER_API_KEY
fi

REGION=${GCP_REGION:-us-central1}
echo "Using region: $REGION"

# Function to check command status
check_status() {
    if [ $? -eq 0 ]; then
        echo "✓ $1 completed successfully"
    else
        echo "✗ $1 failed"
        exit 1
    fi
}

# Step 1: Configure gcloud
echo ""
echo "Step 1: Configuring gcloud..."
gcloud config set project $GCP_PROJECT_ID
check_status "Project configuration"

# Step 2: Enable required APIs
echo ""
echo "Step 2: Enabling required Google Cloud APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    firestore.googleapis.com \
    cloudtasks.googleapis.com \
    cloudbuild.googleapis.com \
    logging.googleapis.com \
    artifactregistry.googleapis.com
check_status "API enablement"

# Step 3: Create Firestore database
echo ""
echo "Step 3: Creating Firestore database..."
gcloud firestore databases create \
    --location=$REGION \
    --type=firestore-native \
    --delete-protection \
    2>/dev/null || echo "Firestore database already exists"

# Step 4: Create Cloud Tasks queue
echo ""
echo "Step 4: Creating Cloud Tasks queue..."
gcloud tasks queues create scraper-queue \
    --location=$REGION \
    --max-concurrent-dispatches=10 \
    --max-dispatches-per-second=1 \
    --max-attempts=3 \
    --min-backoff=30s \
    --max-backoff=300s \
    2>/dev/null || echo "Cloud Tasks queue already exists"

# Step 5: Create service account for functions
echo ""
echo "Step 5: Creating service account..."
SERVICE_ACCOUNT_NAME="business-scraper-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name="Business Scraper Service Account" \
    --description="Service account for business listing scraper functions" \
    2>/dev/null || echo "Service account already exists"

# Grant necessary permissions
echo "Granting permissions to service account..."
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/datastore.user"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/cloudtasks.enqueuer"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/logging.logWriter"

check_status "Service account setup"

# Step 6: Deploy Cloud Functions
echo ""
echo "Step 6: Deploying Cloud Functions..."

# Deploy Empire Flippers scraper
echo ""
echo "Deploying Empire Flippers scraper..."
gcloud functions deploy scrape-empire-flippers \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/scrape_empire_flippers \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT_EMAIL \
    --set-env-vars="SCRAPER_API_KEY=$SCRAPER_API_KEY" \
    --memory=512MB \
    --timeout=300s \
    --max-instances=10 \
    --min-instances=0
check_status "Empire Flippers scraper deployment"

# Deploy BizQuest scraper
echo ""
echo "Deploying BizQuest scraper..."
gcloud functions deploy scrape-bizquest \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/scrape_bizquest \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT_EMAIL \
    --set-env-vars="SCRAPER_API_KEY=$SCRAPER_API_KEY,MAX_DETAIL_PAGES=20" \
    --memory=1GB \
    --timeout=540s \
    --max-instances=10 \
    --min-instances=0
check_status "BizQuest scraper deployment"

# Deploy Orchestrator
echo ""
echo "Deploying Orchestrator..."
gcloud functions deploy orchestrator \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/orchestrator \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT_EMAIL \
    --set-env-vars="GCP_PROJECT=$GCP_PROJECT_ID,FUNCTION_REGION=$REGION" \
    --memory=256MB \
    --timeout=60s \
    --max-instances=5 \
    --min-instances=0
check_status "Orchestrator deployment"

# Deploy API function
echo ""
echo "Deploying API function..."
gcloud functions deploy business-scraper-api \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/api \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT_EMAIL \
    --memory=256MB \
    --timeout=30s \
    --max-instances=20 \
    --min-instances=0
check_status "API deployment"

# Step 7: Create Cloud Scheduler job
echo ""
echo "Step 7: Setting up Cloud Scheduler..."

# Delete existing job if it exists
gcloud scheduler jobs delete daily-business-scraper \
    --location=$REGION \
    --quiet \
    2>/dev/null || true

# Get orchestrator URL
ORCHESTRATOR_URL=$(gcloud functions describe orchestrator \
    --region=$REGION \
    --format="value(serviceConfig.uri)")

# Create scheduler job
gcloud scheduler jobs create http daily-business-scraper \
    --location=$REGION \
    --schedule="0 9 * * *" \
    --time-zone="America/New_York" \
    --description="Trigger daily business listing scraper at 9 AM EST" \
    --uri=$ORCHESTRATOR_URL \
    --http-method=POST \
    --headers="Content-Type=application/json,X-CloudScheduler=true" \
    --message-body='{"trigger":"scheduled","schedule":"daily"}' \
    --attempt-deadline=600s \
    --max-retry-attempts=3 \
    --min-backoff=60s \
    --max-backoff=600s
check_status "Cloud Scheduler setup"

# Step 8: Create Firestore indexes
echo ""
echo "Step 8: Creating Firestore indexes..."
cat > firestore.indexes.json << 'EOF'
{
  "indexes": [
    {
      "collectionGroup": "business_listings",
      "queryScope": "COLLECTION",
      "fields": [
        { "fieldPath": "source", "order": "ASCENDING" },
        { "fieldPath": "is_active", "order": "ASCENDING" },
        { "fieldPath": "last_updated", "order": "DESCENDING" }
      ]
    },
    {
      "collectionGroup": "business_listings",
      "queryScope": "COLLECTION",
      "fields": [
        { "fieldPath": "is_active", "order": "ASCENDING" },
        { "fieldPath": "last_updated", "order": "DESCENDING" }
      ]
    },
    {
      "collectionGroup": "scraper_runs",
      "queryScope": "COLLECTION",
      "fields": [
        { "fieldPath": "source", "order": "ASCENDING" },
        { "fieldPath": "start_time", "order": "DESCENDING" }
      ]
    }
  ]
}
EOF

gcloud firestore indexes create --file=firestore.indexes.json
check_status "Firestore indexes creation"

# Step 9: Set up monitoring alerts (optional)
echo ""
echo "Step 9: Setting up monitoring alerts..."
cat > alert-policy.yaml << EOF
displayName: "Business Scraper Function Errors"
conditions:
  - displayName: "Function error rate"
    conditionThreshold:
      filter: |
        resource.type="cloud_function"
        resource.labels.function_name=~"scrape-.*|orchestrator"
        metric.type="cloudfunctions.googleapis.com/function/error_count"
      comparison: COMPARISON_GT
      thresholdValue: 5
      duration: 300s
notificationChannels: []
alertStrategy:
  autoClose: 86400s
EOF

# Note: To create the alert, you need to set up notification channels first
echo "Alert policy created in alert-policy.yaml. Configure notification channels and create with:"
echo "gcloud alpha monitoring policies create --policy-from-file=alert-policy.yaml"

# Step 10: Display summary
echo ""
echo "==================================="
echo "Setup Complete!"
echo "==================================="
echo ""
echo "Project: $GCP_PROJECT_ID"
echo "Region: $REGION"
echo ""
echo "Deployed Functions:"
gcloud functions list --region=$REGION --format="table(name,state,trigger,runtime)"
echo ""
echo "Scheduler Jobs:"
gcloud scheduler jobs list --location=$REGION
echo ""
echo "API Endpoint:"
echo "$(gcloud functions describe business-scraper-api --region=$REGION --format='value(serviceConfig.uri)')"
echo ""
echo "==================================="
echo "Useful Commands:"
echo "==================================="
echo ""
echo "# Test the orchestrator manually:"
echo "curl -X POST $ORCHESTRATOR_URL"
echo ""
echo "# Trigger the daily scrape immediately:"
echo "gcloud scheduler jobs run daily-business-scraper --location=$REGION"
echo ""
echo "# View function logs:"
echo "gcloud functions logs read --region=$REGION --limit=50"
echo ""
echo "# Query listings via API:"
echo "curl \"$(gcloud functions describe business-scraper-api --region=$REGION --format='value(serviceConfig.uri)')?source=BizQuest&limit=10\""
echo ""
echo "# Check Firestore data in console:"
echo "https://console.cloud.google.com/firestore/data?project=$GCP_PROJECT_ID"
echo ""
echo "==================================="