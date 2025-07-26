# Deployment Guide - Business Listings Scraper

## Prerequisites

1. **Google Cloud Account**: Create one at https://cloud.google.com
2. **Google Cloud SDK**: Install from https://cloud.google.com/sdk/docs/install
3. **ScraperAPI Account**: Get API key from https://www.scraperapi.com

## Setup Instructions

### 1. Create Google Cloud Project

```bash
# Create new project
gcloud projects create YOUR-PROJECT-ID --name="Business Scraper"

# Set as active project
gcloud config set project YOUR-PROJECT-ID

# Enable billing (required for Cloud Functions)
# Visit: https://console.cloud.google.com/billing
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your values
nano .env

# Export variables
export $(cat .env | xargs)
```

### 3. Deploy Infrastructure

```bash
# Run deployment script
./deploy.sh
```

This will:
- Enable required Google Cloud APIs
- Create Firestore database
- Deploy Cloud Functions
- Set up Cloud Scheduler
- Create Cloud Tasks queue

### 4. Verify Deployment

```bash
# List deployed functions
gcloud functions list --region=$GCP_REGION

# Check scheduler job
gcloud scheduler jobs list --location=$GCP_REGION

# Test orchestrator manually
curl -X POST https://$GCP_REGION-$GCP_PROJECT_ID.cloudfunctions.net/orchestrator
```

## Architecture Components

### Cloud Functions

1. **scrape-empire-flippers**: Scrapes Empire Flippers marketplace
2. **scrape-bizquest**: Scrapes BizQuest with detail pages
3. **orchestrator**: Coordinates all scrapers

### Cloud Scheduler

- **daily-business-scraper**: Runs daily at 9 AM EST
- Triggers orchestrator which queues all scrapers

### Firestore Collections

- **business_listings**: Main listings data
- **scraper_runs**: Execution history
- **orchestrations**: Orchestration logs

## Monitoring

### View Logs

```bash
# Function logs
gcloud functions logs read scrape-empire-flippers --region=$GCP_REGION --limit=50

# Scheduler logs
gcloud logging read "resource.type=cloud_scheduler_job" --limit=20
```

### Firestore Console

Visit: https://console.cloud.google.com/firestore

### Metrics Dashboard

1. Go to Cloud Console
2. Navigate to Monitoring > Dashboards
3. Create custom dashboard for:
   - Function invocations
   - Error rates
   - Firestore operations

## Cost Optimization

### Estimated Monthly Costs

- Cloud Functions: ~$5-10 (based on daily runs)
- Firestore: ~$5-20 (depends on data volume)
- Cloud Scheduler: Free (3 jobs free tier)
- ScraperAPI: Based on your plan

### Cost Saving Tips

1. Adjust `MAX_DETAIL_PAGES` environment variable
2. Run scrapers less frequently if needed
3. Use Firestore TTL for old listings
4. Archive to Cloud Storage for long-term storage

## Troubleshooting

### Common Issues

1. **Function timeout**: Increase timeout in deploy script
2. **Rate limits**: Add delays between requests
3. **Authentication errors**: Check service account permissions

### Debug Commands

```bash
# Test function locally
cd functions/scrape_empire_flippers
functions-framework --target=main --debug

# Check function status
gcloud functions describe scrape-empire-flippers --region=$GCP_REGION

# Manually trigger scheduler
gcloud scheduler jobs run daily-business-scraper --location=$GCP_REGION
```

## Security Best Practices

1. **API Keys**: Store in environment variables, not code
2. **Function Access**: Use IAM for production
3. **Firestore Rules**: Implement proper security rules
4. **Monitoring**: Set up alerts for anomalies

## Maintenance

### Update Functions

```bash
# Update single function
gcloud functions deploy scrape-bizquest --source=./functions/scrape_bizquest

# Update all
./deploy.sh
```

### Backup Data

```bash
# Export Firestore to Cloud Storage
gcloud firestore export gs://YOUR-BUCKET/backups/$(date +%Y%m%d)
```

### Clean Old Data

Create a Cloud Function to archive listings older than 90 days to reduce Firestore costs.