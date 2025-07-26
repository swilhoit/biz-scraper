# Business Listings Scraper - Google Cloud Architecture

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Cloud Scheduler │────▶│  Cloud Functions │────▶│ Cloud Firestore │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                           │
                               ▼                           ▼
                        ┌──────────────┐          ┌───────────────┐
                        │ Cloud Storage│          │   BigQuery    │
                        │   (Backups)  │          │  (Analytics)  │
                        └──────────────┘          └───────────────┘
```

## Components

1. **Cloud Scheduler**: Triggers daily scraping jobs
2. **Cloud Functions**: Serverless functions for each marketplace
3. **Cloud Firestore**: NoSQL database for storing listings
4. **Cloud Storage**: Backup and archive storage
5. **BigQuery**: Analytics and reporting (optional)

## Setup Instructions

1. Install Google Cloud SDK
2. Create a new GCP project
3. Enable required APIs
4. Deploy the infrastructure

See deployment guide in `deployment.md`