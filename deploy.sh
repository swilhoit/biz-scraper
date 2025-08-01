#!/bin/bash

# Business Scraper Deployment Script for Google Cloud
# This script helps deploy the scraper to various GCP services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
PROJECT_ID=""
REGION="us-central1"
SERVICE_NAME="biz-scraper"
DEPLOYMENT_TYPE="cloud-run"

# Function to print colored output
print_color() {
    COLOR=$1
    MESSAGE=$2
    echo -e "${COLOR}${MESSAGE}${NC}"
}

# Function to check if gcloud is installed
check_gcloud() {
    if ! command -v gcloud &> /dev/null; then
        print_color $RED "Error: gcloud CLI is not installed. Please install it first."
        echo "Visit: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
}

# Function to get current project
get_current_project() {
    CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
    if [ -z "$CURRENT_PROJECT" ]; then
        print_color $RED "No project is currently set."
        return 1
    fi
    echo "$CURRENT_PROJECT"
    return 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project)
            PROJECT_ID="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --type)
            DEPLOYMENT_TYPE="$2"
            shift 2
            ;;
        --service-name)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --help)
            echo "Usage: ./deploy.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --project PROJECT_ID      GCP Project ID"
            echo "  --region REGION          GCP Region (default: us-central1)"
            echo "  --type TYPE              Deployment type: cloud-run, app-engine, or cloud-build (default: cloud-run)"
            echo "  --service-name NAME      Service name (default: biz-scraper)"
            echo "  --help                   Show this help message"
            exit 0
            ;;
        *)
            print_color $RED "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if gcloud is installed
check_gcloud

# Set project if not provided
if [ -z "$PROJECT_ID" ]; then
    print_color $YELLOW "No project ID provided. Checking current gcloud configuration..."
    PROJECT_ID=$(get_current_project)
    if [ $? -ne 0 ]; then
        print_color $RED "Please provide a project ID using --project flag"
        exit 1
    fi
    print_color $GREEN "Using project: $PROJECT_ID"
fi

# Set the project
gcloud config set project $PROJECT_ID

print_color $GREEN "Starting deployment process..."
print_color $YELLOW "Project: $PROJECT_ID"
print_color $YELLOW "Region: $REGION"
print_color $YELLOW "Deployment Type: $DEPLOYMENT_TYPE"
print_color $YELLOW "Service Name: $SERVICE_NAME"

# Check if .env file exists
if [ ! -f .env ]; then
    print_color $RED "Error: .env file not found!"
    print_color $YELLOW "Please create a .env file with your SCRAPER_API_KEY"
    exit 1
fi

# Create secret for API key if it doesn't exist
print_color $YELLOW "Checking/Creating secret for SCRAPER_API_KEY..."
if ! gcloud secrets describe scraper-api-key --project=$PROJECT_ID &>/dev/null; then
    print_color $YELLOW "Creating secret scraper-api-key..."
    # Extract SCRAPER_API_KEY from .env file
    SCRAPER_API_KEY=$(grep SCRAPER_API_KEY .env | cut -d '=' -f2)
    echo -n "$SCRAPER_API_KEY" | gcloud secrets create scraper-api-key \
        --data-file=- \
        --project=$PROJECT_ID
    print_color $GREEN "Secret created successfully!"
else
    print_color $GREEN "Secret scraper-api-key already exists."
fi

# Enable required APIs
print_color $YELLOW "Enabling required Google Cloud APIs..."
gcloud services enable cloudbuild.googleapis.com --project=$PROJECT_ID
gcloud services enable run.googleapis.com --project=$PROJECT_ID
gcloud services enable containerregistry.googleapis.com --project=$PROJECT_ID
gcloud services enable secretmanager.googleapis.com --project=$PROJECT_ID
gcloud services enable bigquery.googleapis.com --project=$PROJECT_ID

case $DEPLOYMENT_TYPE in
    "cloud-run")
        print_color $GREEN "Deploying to Cloud Run..."
        
        # Build the Docker image
        print_color $YELLOW "Building Docker image..."
        gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME --project=$PROJECT_ID
        
        # Deploy to Cloud Run
        print_color $YELLOW "Deploying to Cloud Run..."
        gcloud run deploy $SERVICE_NAME \
            --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
            --platform managed \
            --region $REGION \
            --allow-unauthenticated \
            --memory 4Gi \
            --cpu 2 \
            --timeout 3600 \
            --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET_NAME=business_listings" \
            --set-secrets "SCRAPER_API_KEY=scraper-api-key:latest" \
            --project=$PROJECT_ID
            
        print_color $GREEN "Cloud Run deployment completed!"
        ;;
        
    "app-engine")
        print_color $GREEN "Deploying to App Engine..."
        
        # Update app.yaml with project ID
        sed -i.bak "s/YOUR_PROJECT_ID/$PROJECT_ID/g" app.yaml
        
        # Deploy to App Engine
        gcloud app deploy app.yaml --project=$PROJECT_ID
        
        # Restore original app.yaml
        mv app.yaml.bak app.yaml
        
        print_color $GREEN "App Engine deployment completed!"
        ;;
        
    "cloud-build")
        print_color $GREEN "Triggering Cloud Build..."
        
        # Submit build using cloudbuild.yaml
        gcloud builds submit --config cloudbuild.yaml --project=$PROJECT_ID
        
        print_color $GREEN "Cloud Build completed!"
        ;;
        
    *)
        print_color $RED "Invalid deployment type: $DEPLOYMENT_TYPE"
        print_color $YELLOW "Valid options: cloud-run, app-engine, cloud-build"
        exit 1
        ;;
esac

# Create Cloud Scheduler job for periodic runs (optional)
print_color $YELLOW "Do you want to create a Cloud Scheduler job for periodic scraping? (y/n)"
read -r CREATE_SCHEDULER

if [[ $CREATE_SCHEDULER == "y" ]]; then
    print_color $YELLOW "Enabling Cloud Scheduler API..."
    gcloud services enable cloudscheduler.googleapis.com --project=$PROJECT_ID
    
    if [ "$DEPLOYMENT_TYPE" == "cloud-run" ]; then
        SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)' --project=$PROJECT_ID)
        
        print_color $YELLOW "Creating Cloud Scheduler job to run daily at 2 AM..."
        gcloud scheduler jobs create http scraper-daily \
            --location=$REGION \
            --schedule="0 2 * * *" \
            --uri="$SERVICE_URL" \
            --http-method=GET \
            --project=$PROJECT_ID
            
        print_color $GREEN "Cloud Scheduler job created!"
    else
        print_color $YELLOW "Cloud Scheduler setup is only supported for Cloud Run deployments."
    fi
fi

print_color $GREEN "Deployment completed successfully!"
print_color $YELLOW ""
print_color $YELLOW "Next steps:"
print_color $YELLOW "1. Verify your BigQuery dataset is created: business_listings"
print_color $YELLOW "2. Check the logs in Cloud Console"
print_color $YELLOW "3. Monitor the scraper execution"

if [ "$DEPLOYMENT_TYPE" == "cloud-run" ]; then
    SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)' --project=$PROJECT_ID)
    print_color $GREEN "Your service is available at: $SERVICE_URL"
fi