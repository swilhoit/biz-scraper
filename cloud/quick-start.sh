#!/bin/bash

# Quick start script for Business Scraper deployment

cat << 'EOF'
╔════════════════════════════════════════════╗
║     Business Scraper - Quick Start         ║
╚════════════════════════════════════════════╝

This script will deploy the complete Business Scraper system to Google Cloud.

Prerequisites:
- Google Cloud SDK installed
- Active GCP project with billing enabled
- ScraperAPI key

EOF

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI is not installed."
    echo "Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get project ID
echo "Step 1: Project Configuration"
echo "----------------------------"
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -n "$CURRENT_PROJECT" ]; then
    read -p "Use current project '$CURRENT_PROJECT'? (y/n): " USE_CURRENT
    if [[ $USE_CURRENT != "y" ]]; then
        read -p "Enter your GCP Project ID: " GCP_PROJECT_ID
    else
        GCP_PROJECT_ID=$CURRENT_PROJECT
    fi
else
    read -p "Enter your GCP Project ID: " GCP_PROJECT_ID
fi

# Get ScraperAPI key
echo ""
echo "Step 2: API Configuration"
echo "------------------------"
read -p "Enter your ScraperAPI Key: " SCRAPER_API_KEY

# Set region
echo ""
echo "Step 3: Region Selection"
echo "-----------------------"
echo "Available regions:"
echo "1. us-central1 (Iowa)"
echo "2. us-east1 (South Carolina)"
echo "3. europe-west1 (Belgium)"
echo "4. asia-northeast1 (Tokyo)"
read -p "Select region (1-4) [default: 1]: " REGION_CHOICE

case $REGION_CHOICE in
    2) REGION="us-east1" ;;
    3) REGION="europe-west1" ;;
    4) REGION="asia-northeast1" ;;
    *) REGION="us-central1" ;;
esac

echo ""
echo "Configuration Summary:"
echo "--------------------"
echo "Project ID: $GCP_PROJECT_ID"
echo "Region: $REGION"
echo "ScraperAPI Key: ${SCRAPER_API_KEY:0:10}..."
echo ""
read -p "Proceed with deployment? (y/n): " PROCEED

if [[ $PROCEED != "y" ]]; then
    echo "Deployment cancelled."
    exit 0
fi

# Export variables
export GCP_PROJECT_ID=$GCP_PROJECT_ID
export SCRAPER_API_KEY=$SCRAPER_API_KEY
export GCP_REGION=$REGION

# Run main setup
echo ""
echo "Starting deployment..."
echo ""

# Check if we're in the cloud directory
if [ ! -f "setup-gcloud.sh" ]; then
    echo "Error: Please run this script from the cloud directory"
    exit 1
fi

# Run the main setup script
./setup-gcloud.sh

# Optional: Set up monitoring
echo ""
read -p "Set up monitoring and alerts? (y/n): " SETUP_MONITORING
if [[ $SETUP_MONITORING == "y" ]]; then
    ./setup-monitoring.sh
fi

echo ""
echo "╔════════════════════════════════════════════╗"
echo "║        Deployment Complete! 🎉              ║"
echo "╚════════════════════════════════════════════╝"
echo ""
echo "Your Business Scraper is now deployed!"
echo ""
echo "Test the system:"
echo "---------------"
echo "# Trigger a manual scrape:"
echo "gcloud scheduler jobs run daily-business-scraper --location=$REGION"
echo ""
echo "# View the API:"
echo "API_URL=\$(gcloud functions describe business-scraper-api --region=$REGION --format='value(serviceConfig.uri)')"
echo "curl \"\$API_URL?limit=10\""
echo ""
echo "# View logs:"
echo "gcloud functions logs read --region=$REGION --limit=20"
echo ""
echo "# Access Firestore console:"
echo "https://console.cloud.google.com/firestore/data?project=$GCP_PROJECT_ID"
echo ""