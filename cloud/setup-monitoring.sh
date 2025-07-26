#!/bin/bash

# Set up monitoring and alerting for Business Scraper

set -e

PROJECT_ID=${GCP_PROJECT_ID:-$(gcloud config get-value project)}

echo "Setting up monitoring for project: $PROJECT_ID"

# Create notification channel (email)
echo "Creating notification channel..."
read -p "Enter email for alerts: " ALERT_EMAIL

# Create notification channel
CHANNEL_ID=$(gcloud alpha monitoring channels create \
    --display-name="Business Scraper Alerts" \
    --type=email \
    --channel-labels=email_address=$ALERT_EMAIL \
    --format="value(name)")

echo "Created notification channel: $CHANNEL_ID"

# Create alert policies
echo ""
echo "Creating alert policies..."

# Alert 1: Function errors
cat > function-error-policy.yaml << EOF
displayName: "Business Scraper - Function Errors"
documentation:
  content: |
    This alert triggers when scraper functions encounter errors.
    Check the function logs for details.
conditions:
  - displayName: "High error rate in scraper functions"
    conditionThreshold:
      filter: |
        resource.type="cloud_function"
        resource.labels.function_name=~"scrape-.*|orchestrator"
        metric.type="cloudfunctions.googleapis.com/function/error_count"
      comparison: COMPARISON_GT
      thresholdValue: 5
      duration: 300s
      aggregations:
        - alignmentPeriod: 60s
          perSeriesAligner: ALIGN_RATE
notificationChannels:
  - $CHANNEL_ID
alertStrategy:
  autoClose: 86400s
EOF

gcloud alpha monitoring policies create --policy-from-file=function-error-policy.yaml

# Alert 2: Function execution time
cat > function-latency-policy.yaml << EOF
displayName: "Business Scraper - Slow Function Execution"
documentation:
  content: |
    This alert triggers when scraper functions take too long to execute.
    This might indicate API issues or rate limiting.
conditions:
  - displayName: "Function execution exceeding 5 minutes"
    conditionThreshold:
      filter: |
        resource.type="cloud_function"
        resource.labels.function_name=~"scrape-.*"
        metric.type="cloudfunctions.googleapis.com/function/execution_times"
      comparison: COMPARISON_GT
      thresholdValue: 300000  # 5 minutes in milliseconds
      duration: 60s
      aggregations:
        - alignmentPeriod: 60s
          perSeriesAligner: ALIGN_PERCENTILE_95
notificationChannels:
  - $CHANNEL_ID
alertStrategy:
  autoClose: 3600s
EOF

gcloud alpha monitoring policies create --policy-from-file=function-latency-policy.yaml

# Alert 3: Scheduler job failures
cat > scheduler-failure-policy.yaml << EOF
displayName: "Business Scraper - Scheduler Job Failures"
documentation:
  content: |
    This alert triggers when the daily scheduler job fails.
    Check Cloud Scheduler logs for details.
conditions:
  - displayName: "Scheduler job failed"
    conditionThreshold:
      filter: |
        resource.type="cloud_scheduler_job"
        resource.labels.job_id="daily-business-scraper"
        metric.type="logging.googleapis.com/user/scheduler_job_failed"
      comparison: COMPARISON_GT
      thresholdValue: 0
      duration: 60s
notificationChannels:
  - $CHANNEL_ID
alertStrategy:
  autoClose: 3600s
EOF

# Create log-based metric for scheduler failures first
gcloud logging metrics create scheduler_job_failed \
    --description="Count of failed scheduler job executions" \
    --log-filter='resource.type="cloud_scheduler_job"
    resource.labels.job_id="daily-business-scraper"
    jsonPayload.@type="type.googleapis.com/google.cloud.scheduler.logging.AttemptFinished"
    jsonPayload.status!="OK"'

# Then create the alert policy
gcloud alpha monitoring policies create --policy-from-file=scheduler-failure-policy.yaml

# Alert 4: Firestore quota usage
cat > firestore-quota-policy.yaml << EOF
displayName: "Business Scraper - Firestore Quota Warning"
documentation:
  content: |
    This alert triggers when Firestore operations approach daily quotas.
    Consider optimizing queries or increasing quotas.
conditions:
  - displayName: "High Firestore read operations"
    conditionThreshold:
      filter: |
        resource.type="firestore.googleapis.com/Database"
        metric.type="firestore.googleapis.com/document/read_count"
      comparison: COMPARISON_GT
      thresholdValue: 40000  # 80% of free tier daily limit
      duration: 3600s
      aggregations:
        - alignmentPeriod: 3600s
          perSeriesAligner: ALIGN_RATE
notificationChannels:
  - $CHANNEL_ID
alertStrategy:
  autoClose: 86400s
EOF

gcloud alpha monitoring policies create --policy-from-file=firestore-quota-policy.yaml

# Create custom dashboard
echo ""
echo "Creating monitoring dashboard..."

cat > dashboard.json << 'EOF'
{
  "displayName": "Business Scraper Dashboard",
  "mosaicLayout": {
    "columns": 12,
    "tiles": [
      {
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Function Invocations",
          "xyChart": {
            "dataSets": [{
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"cloud_function\" resource.labels.function_name=~\"scrape-.*|orchestrator\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_RATE",
                    "crossSeriesReducer": "REDUCE_SUM",
                    "groupByFields": ["resource.label.function_name"]
                  }
                }
              }
            }]
          }
        }
      },
      {
        "xPos": 6,
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Function Errors",
          "xyChart": {
            "dataSets": [{
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"cloud_function\" metric.type=\"cloudfunctions.googleapis.com/function/error_count\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_RATE",
                    "crossSeriesReducer": "REDUCE_SUM",
                    "groupByFields": ["resource.label.function_name"]
                  }
                }
              }
            }]
          }
        }
      },
      {
        "yPos": 4,
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Function Execution Time (95th percentile)",
          "xyChart": {
            "dataSets": [{
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"cloud_function\" metric.type=\"cloudfunctions.googleapis.com/function/execution_times\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_PERCENTILE_95",
                    "crossSeriesReducer": "REDUCE_MEAN",
                    "groupByFields": ["resource.label.function_name"]
                  }
                }
              }
            }]
          }
        }
      },
      {
        "xPos": 6,
        "yPos": 4,
        "width": 6,
        "height": 4,
        "widget": {
          "title": "Firestore Operations",
          "xyChart": {
            "dataSets": [
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "resource.type=\"firestore.googleapis.com/Database\" metric.type=\"firestore.googleapis.com/document/write_count\"",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"
                    }
                  }
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              },
              {
                "timeSeriesQuery": {
                  "timeSeriesFilter": {
                    "filter": "resource.type=\"firestore.googleapis.com/Database\" metric.type=\"firestore.googleapis.com/document/read_count\"",
                    "aggregation": {
                      "alignmentPeriod": "60s",
                      "perSeriesAligner": "ALIGN_RATE"
                    }
                  }
                },
                "plotType": "LINE",
                "targetAxis": "Y1"
              }
            ]
          }
        }
      }
    ]
  }
}
EOF

gcloud monitoring dashboards create --config-from-file=dashboard.json

# Create uptime checks
echo ""
echo "Creating uptime checks..."

# Get function URLs
API_URL=$(gcloud functions describe business-scraper-api --region=us-central1 --format="value(serviceConfig.uri)")

gcloud monitoring uptime-check-configs create \
    --display-name="Business Scraper API Health" \
    --resource-type="URL" \
    --monitored-resource-labels="project_id=$PROJECT_ID,host=${API_URL#https://},path=/" \
    --http-check-path="/" \
    --check-frequency=300

# Create SLOs (Service Level Objectives)
echo ""
echo "Creating SLOs..."

cat > api-slo.yaml << EOF
displayName: "Business Scraper API - Availability SLO"
serviceLevelIndicator:
  requestBased:
    goodTotalRatio:
      goodServiceFilter: |
        resource.type="cloud_function"
        resource.labels.function_name="business-scraper-api"
        metric.type="cloudfunctions.googleapis.com/function/execution_count"
        metric.labels.status!="error"
      totalServiceFilter: |
        resource.type="cloud_function"
        resource.labels.function_name="business-scraper-api"
        metric.type="cloudfunctions.googleapis.com/function/execution_count"
goal: 0.99
rollingPeriod: 604800s  # 7 days
EOF

# Note: SLO creation requires the Service Monitoring API
echo "SLO configuration saved to api-slo.yaml"
echo "To create SLO, enable Service Monitoring API and use the Cloud Console"

# Summary
echo ""
echo "======================================"
echo "Monitoring Setup Complete!"
echo "======================================"
echo ""
echo "Created:"
echo "- Email notification channel"
echo "- 4 alert policies"
echo "- Custom dashboard"
echo "- Uptime check for API"
echo ""
echo "View in Cloud Console:"
echo "- Alerts: https://console.cloud.google.com/monitoring/alerting/policies?project=$PROJECT_ID"
echo "- Dashboard: https://console.cloud.google.com/monitoring/dashboards?project=$PROJECT_ID"
echo "- Uptime: https://console.cloud.google.com/monitoring/uptime?project=$PROJECT_ID"
echo ""
echo "Alert policies created:"
gcloud alpha monitoring policies list --format="table(displayName,enabled)"
echo ""
echo "To test alerts:"
echo "gcloud functions call scrape-empire-flippers --data='{}' --region=us-central1"