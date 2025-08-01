# Google Cloud Deployment Guide

This guide explains how to deploy the Business Scraper application to Google Cloud Platform.

## Prerequisites

1. **Google Cloud Account**: Create one at https://cloud.google.com
2. **gcloud CLI**: Install from https://cloud.google.com/sdk/docs/install
3. **Project Setup**: Create a new GCP project or use an existing one
4. **BigQuery API**: Will be enabled automatically by the deployment script
5. **Environment Variables**: Create a `.env` file with your `SCRAPER_API_KEY`

## Files Created for Deployment

1. **Dockerfile**: Containerizes the application for Cloud Run/Kubernetes
2. **app.yaml**: Configuration for App Engine Flexible Environment
3. **cloudbuild.yaml**: Automated build configuration for Cloud Build
4. **.gcloudignore**: Specifies files to exclude from deployment
5. **deploy.sh**: Automated deployment script
6. **requirements.txt**: Updated with Google Cloud dependencies

## Deployment Options

### Option 1: Cloud Run (Recommended)
Best for: Serverless deployment, automatic scaling, pay-per-use

```bash
./deploy.sh --project YOUR_PROJECT_ID --type cloud-run
```

### Option 2: App Engine Flexible
Best for: Managed platform, automatic scaling, scheduled tasks

```bash
./deploy.sh --project YOUR_PROJECT_ID --type app-engine
```

### Option 3: Cloud Build Only
Best for: Building images for manual deployment

```bash
./deploy.sh --project YOUR_PROJECT_ID --type cloud-build
```

## Quick Start

1. **Authenticate with Google Cloud**:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```

2. **Set your project ID**:
   ```bash
   export PROJECT_ID=your-project-id
   ```

3. **Run the deployment**:
   ```bash
   ./deploy.sh --project $PROJECT_ID
   ```

## Manual Deployment Steps

### Cloud Run Deployment

1. **Build the Docker image**:
   ```bash
   gcloud builds submit --tag gcr.io/$PROJECT_ID/biz-scraper
   ```

2. **Deploy to Cloud Run**:
   ```bash
   gcloud run deploy biz-scraper \
     --image gcr.io/$PROJECT_ID/biz-scraper \
     --platform managed \
     --region us-central1 \
     --memory 4Gi \
     --cpu 2 \
     --timeout 3600 \
     --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET_NAME=business_listings"
   ```

### App Engine Deployment

1. **Update app.yaml** with your project details
2. **Deploy**:
   ```bash
   gcloud app deploy app.yaml
   ```

## Setting Up Secrets

The deployment script automatically creates a secret for your SCRAPER_API_KEY. To manage it manually:

```bash
# Create secret
echo -n "your-api-key" | gcloud secrets create scraper-api-key --data-file=-

# Grant access
gcloud secrets add-iam-policy-binding scraper-api-key \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT" \
  --role="roles/secretmanager.secretAccessor"
```

## Scheduling Periodic Runs

For Cloud Run, you can set up Cloud Scheduler:

```bash
gcloud scheduler jobs create http scraper-daily \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --uri="YOUR_CLOUD_RUN_URL" \
  --http-method=GET
```

## Monitoring

1. **View logs**:
   ```bash
   gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=biz-scraper" --limit 50
   ```

2. **Check BigQuery**:
   - Go to BigQuery Console
   - Navigate to your project
   - Check the `business_listings` dataset

## Cost Optimization

- Cloud Run charges only for request processing time
- Set appropriate memory/CPU limits
- Use Cloud Scheduler for periodic runs instead of keeping services running
- Monitor usage in the GCP Console

## Troubleshooting

1. **Authentication errors**: Run `gcloud auth application-default login`
2. **Permission errors**: Ensure required APIs are enabled
3. **Secret access errors**: Check IAM permissions for the service account
4. **BigQuery errors**: Verify dataset exists and service account has BigQuery permissions

## Security Best Practices

1. Never commit `.env` files or secrets to version control
2. Use Secret Manager for all sensitive data
3. Restrict service account permissions to minimum required
4. Enable audit logging for production deployments