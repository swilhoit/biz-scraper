#!/bin/bash

# Business Listings Scraper - Google Cloud Deployment Script

set -e

# Configuration
PROJECT_ID=${GCP_PROJECT_ID}
REGION=${GCP_REGION:-us-central1}
SCRAPER_API_KEY=${SCRAPER_API_KEY}

if [ -z "$PROJECT_ID" ]; then
    echo "Error: GCP_PROJECT_ID environment variable is not set"
    exit 1
fi

if [ -z "$SCRAPER_API_KEY" ]; then
    echo "Error: SCRAPER_API_KEY environment variable is not set"
    exit 1
fi

echo "Deploying to project: $PROJECT_ID in region: $REGION"

# Set the project
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "Enabling required APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    firestore.googleapis.com \
    cloudtasks.googleapis.com \
    cloudbuild.googleapis.com \
    logging.googleapis.com

# Create Firestore database if it doesn't exist
echo "Setting up Firestore..."
gcloud firestore databases create --region=$REGION --type=firestore-native || true

# Create Cloud Tasks queue
echo "Creating Cloud Tasks queue..."
gcloud tasks queues create scraper-queue \
    --location=$REGION \
    --max-concurrent-dispatches=10 \
    --max-dispatches-per-second=1 || true

# Deploy Cloud Functions
echo "Deploying Cloud Functions..."

# Deploy Empire Flippers scraper
echo "Deploying Empire Flippers scraper..."
gcloud functions deploy scrape-empire-flippers \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/scrape_empire_flippers \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="SCRAPER_API_KEY=$SCRAPER_API_KEY" \
    --memory=512MB \
    --timeout=300s

# Deploy BizQuest scraper
echo "Deploying BizQuest scraper..."
gcloud functions deploy scrape-bizquest \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/scrape_bizquest \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="SCRAPER_API_KEY=$SCRAPER_API_KEY,MAX_DETAIL_PAGES=20" \
    --memory=1GB \
    --timeout=540s

# Deploy Orchestrator
echo "Deploying Orchestrator..."
gcloud functions deploy orchestrator \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=./functions/orchestrator \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="GCP_PROJECT=$PROJECT_ID,FUNCTION_REGION=$REGION" \
    --memory=256MB \
    --timeout=60s

# Create Cloud Scheduler job
echo "Setting up Cloud Scheduler..."
ORCHESTRATOR_URL="https://$REGION-$PROJECT_ID.cloudfunctions.net/orchestrator"

gcloud scheduler jobs delete daily-business-scraper --location=$REGION --quiet || true

gcloud scheduler jobs create http daily-business-scraper \
    --location=$REGION \
    --schedule="0 9 * * *" \
    --time-zone="America/New_York" \
    --uri=$ORCHESTRATOR_URL \
    --http-method=POST \
    --headers="Content-Type=application/json,X-CloudScheduler=true" \
    --message-body='{"trigger":"scheduled","schedule":"daily"}' \
    --attempt-deadline=600s

echo "Deployment complete!"
echo ""
echo "Next steps:"
echo "1. Test the orchestrator: curl -X POST $ORCHESTRATOR_URL"
echo "2. Check Cloud Scheduler: gcloud scheduler jobs list --location=$REGION"
echo "3. View logs: gcloud functions logs read --region=$REGION"
echo ""
echo "To manually trigger the daily scrape:"
echo "gcloud scheduler jobs run daily-business-scraper --location=$REGION"