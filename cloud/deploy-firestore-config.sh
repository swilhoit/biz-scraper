#!/bin/bash

# Deploy Firestore configuration using gcloud

set -e

echo "Deploying Firestore configuration..."

# Deploy security rules
echo "Deploying Firestore security rules..."
gcloud firestore rules update firestore.rules

# Deploy indexes
echo "Deploying Firestore indexes..."
gcloud firestore indexes create --file=firestore.indexes.json

# Check deployment status
echo ""
echo "Checking deployment status..."
echo "Security rules:"
gcloud firestore rules describe

echo ""
echo "Indexes:"
gcloud firestore indexes list

echo ""
echo "Active operations:"
gcloud firestore operations list --filter="done=false"

echo ""
echo "Firestore configuration deployed successfully!"