# GCloud CLI Commands Reference

## Initial Setup

### 1. Authenticate and Set Project
```bash
# Login to Google Cloud
gcloud auth login

# List available projects
gcloud projects list

# Set active project
gcloud config set project YOUR-PROJECT-ID

# Set default region
gcloud config set functions/region us-central1
```

### 2. Enable Required APIs
```bash
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    firestore.googleapis.com \
    cloudtasks.googleapis.com \
    cloudbuild.googleapis.com \
    logging.googleapis.com \
    artifactregistry.googleapis.com
```

### 3. Create Firestore Database
```bash
# Create Firestore in Native mode
gcloud firestore databases create \
    --location=us-central1 \
    --type=firestore-native \
    --delete-protection
```

## Deploy Functions

### Deploy Empire Flippers Scraper
```bash
gcloud functions deploy scrape-empire-flippers \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=./functions/scrape_empire_flippers \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="SCRAPER_API_KEY=YOUR_API_KEY" \
    --memory=512MB \
    --timeout=300s \
    --max-instances=10
```

### Deploy BizQuest Scraper
```bash
gcloud functions deploy scrape-bizquest \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=./functions/scrape_bizquest \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="SCRAPER_API_KEY=YOUR_API_KEY,MAX_DETAIL_PAGES=20" \
    --memory=1GB \
    --timeout=540s \
    --max-instances=10
```

### Deploy Orchestrator
```bash
gcloud functions deploy orchestrator \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=./functions/orchestrator \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars="GCP_PROJECT=YOUR-PROJECT-ID,FUNCTION_REGION=us-central1" \
    --memory=256MB \
    --timeout=60s
```

### Deploy API
```bash
gcloud functions deploy business-scraper-api \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=./functions/api \
    --entry-point=main \
    --trigger-http \
    --allow-unauthenticated \
    --memory=256MB \
    --timeout=30s
```

## Cloud Scheduler Setup

### Create Cloud Tasks Queue
```bash
gcloud tasks queues create scraper-queue \
    --location=us-central1 \
    --max-concurrent-dispatches=10 \
    --max-dispatches-per-second=1 \
    --max-attempts=3 \
    --min-backoff=30s \
    --max-backoff=300s
```

### Create Daily Schedule
```bash
# Get the orchestrator URL
ORCHESTRATOR_URL=$(gcloud functions describe orchestrator \
    --region=us-central1 \
    --format="value(serviceConfig.uri)")

# Create scheduler job
gcloud scheduler jobs create http daily-business-scraper \
    --location=us-central1 \
    --schedule="0 9 * * *" \
    --time-zone="America/New_York" \
    --uri=$ORCHESTRATOR_URL \
    --http-method=POST \
    --headers="Content-Type=application/json,X-CloudScheduler=true" \
    --message-body='{"trigger":"scheduled","schedule":"daily"}' \
    --attempt-deadline=600s
```

## Service Account Management

### Create Service Account
```bash
# Create service account
gcloud iam service-accounts create business-scraper-sa \
    --display-name="Business Scraper Service Account"

# Grant Firestore access
gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
    --member="serviceAccount:business-scraper-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com" \
    --role="roles/datastore.user"

# Grant Cloud Tasks access
gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
    --member="serviceAccount:business-scraper-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com" \
    --role="roles/cloudtasks.enqueuer"
```

## Monitoring & Logs

### View Function Logs
```bash
# View all function logs
gcloud functions logs read --region=us-central1 --limit=50

# View specific function logs
gcloud functions logs read scrape-empire-flippers \
    --region=us-central1 \
    --limit=20

# Stream logs in real-time
gcloud functions logs read scrape-bizquest \
    --region=us-central1 \
    --limit=50 \
    --format="value(log)" \
    --filter="severity>=ERROR"
```

### View Scheduler Logs
```bash
# View scheduler job executions
gcloud logging read \
    "resource.type=cloud_scheduler_job AND \
     resource.labels.job_id=daily-business-scraper" \
    --limit=10 \
    --format=json
```

## Testing & Debugging

### Test Functions Locally
```bash
# Test function locally with Functions Framework
cd functions/scrape_empire_flippers
pip install functions-framework
functions-framework --target=main --debug --port=8080
```

### Trigger Functions Manually
```bash
# Trigger orchestrator
curl -X POST $(gcloud functions describe orchestrator \
    --region=us-central1 \
    --format="value(serviceConfig.uri)")

# Trigger specific scraper
curl -X POST $(gcloud functions describe scrape-empire-flippers \
    --region=us-central1 \
    --format="value(serviceConfig.uri)")

# Trigger scheduler job manually
gcloud scheduler jobs run daily-business-scraper --location=us-central1
```

### Query API
```bash
# Get API URL
API_URL=$(gcloud functions describe business-scraper-api \
    --region=us-central1 \
    --format="value(serviceConfig.uri)")

# Query all listings
curl "$API_URL"

# Query with filters
curl "$API_URL?source=BizQuest&limit=10&has_revenue=true"
```

## Maintenance

### Update Function
```bash
# Update single function
gcloud functions deploy scrape-bizquest \
    --source=./functions/scrape_bizquest \
    --update-env-vars="MAX_DETAIL_PAGES=50"

# Update environment variables only
gcloud functions deploy orchestrator \
    --update-env-vars="DEBUG=true"

# Remove environment variable
gcloud functions deploy orchestrator \
    --remove-env-vars="DEBUG"
```

### Delete Resources
```bash
# Delete function
gcloud functions delete scrape-empire-flippers --region=us-central1

# Delete scheduler job
gcloud scheduler jobs delete daily-business-scraper --location=us-central1

# Delete Cloud Tasks queue
gcloud tasks queues delete scraper-queue --location=us-central1
```

## Firestore Management

### Create Indexes
```bash
# Deploy indexes from file
gcloud firestore indexes create --file=firestore.indexes.json

# List existing indexes
gcloud firestore indexes list

# Monitor index creation
gcloud firestore operations list
```

### Export/Import Data
```bash
# Create backup bucket
gsutil mb gs://YOUR-PROJECT-ID-backups

# Export Firestore
gcloud firestore export gs://YOUR-PROJECT-ID-backups/$(date +%Y%m%d)

# Import Firestore
gcloud firestore import gs://YOUR-PROJECT-ID-backups/20240114
```

## Cost Management

### View Billing
```bash
# Get current billing account
gcloud billing accounts list

# View project billing info
gcloud billing projects describe YOUR-PROJECT-ID
```

### Set Budget Alerts
```bash
# Create budget with gcloud (requires billing API)
gcloud billing budgets create \
    --billing-account=YOUR-BILLING-ACCOUNT \
    --display-name="Business Scraper Budget" \
    --budget-amount=50 \
    --threshold-rule=percent=0.5 \
    --threshold-rule=percent=0.9 \
    --threshold-rule=percent=1.0
```

## Debugging Common Issues

### Function Deployment Fails
```bash
# Check Cloud Build logs
gcloud builds list --limit=5

# Get detailed build logs
gcloud builds log BUILD-ID

# Check service account permissions
gcloud projects get-iam-policy YOUR-PROJECT-ID
```

### Scheduler Not Triggering
```bash
# Check scheduler job status
gcloud scheduler jobs describe daily-business-scraper \
    --location=us-central1

# Test with CURL to ensure function is accessible
curl -I $(gcloud functions describe orchestrator \
    --region=us-central1 \
    --format="value(serviceConfig.uri)")

# Check for paused jobs
gcloud scheduler jobs list --location=us-central1 --filter="state:PAUSED"
```

### Firestore Permission Issues
```bash
# Check current permissions
gcloud projects get-iam-policy YOUR-PROJECT-ID \
    --flatten="bindings[].members" \
    --filter="bindings.role:roles/datastore.user"

# Grant missing permissions
gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
    --member="serviceAccount:YOUR-SA@YOUR-PROJECT-ID.iam.gserviceaccount.com" \
    --role="roles/datastore.user"
```