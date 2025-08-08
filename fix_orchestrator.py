#!/usr/bin/env python3
"""
Fix for the orchestrator Cloud Function to correct the trigger_type field issue.
The function is trying to insert trigger_type as a record but BigQuery expects a string.
"""

import functions_framework
from google.cloud import bigquery
from datetime import datetime
import json
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def orchestrator(request):
    """HTTP Cloud Function to orchestrate scraping tasks."""
    
    # Get project ID from environment or default
    project_id = os.environ.get('GCP_PROJECT_ID', 'biz-hunter-oauth')
    dataset_id = 'business_data'
    table_id = 'scraper_runs'
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Parse request data
    request_json = request.get_json(silent=True)
    
    # Extract trigger information
    trigger_source = "scheduler"  # Default to scheduler
    if request_json and 'trigger_type' in request_json:
        # If trigger_type is passed as a dict/record, extract the string value
        if isinstance(request_json['trigger_type'], dict):
            trigger_source = request_json['trigger_type'].get('type', 'scheduler')
        else:
            trigger_source = str(request_json['trigger_type'])
    
    # Create the run record
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    scraper_run = {
        'run_id': run_id,
        'source': 'orchestrator',
        'start_time': datetime.utcnow().isoformat(),
        'status': 'running',
        'trigger_type': trigger_source,  # This should be a string, not a record
        'scraper_version': '1.0.1',
        'ingestion_date': datetime.utcnow().date().isoformat()
    }
    
    try:
        # Insert the record into BigQuery
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        errors = client.insert_rows_json(table, [scraper_run])
        
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            return json.dumps({'error': 'Failed to insert run record', 'details': str(errors)}), 500
        
        logger.info(f"Successfully created scraper run: {run_id}")
        
        # TODO: Trigger individual scraper functions here
        # For now, just return success
        
        # Update the run record to completed
        update_query = f"""
        UPDATE `{project_id}.{dataset_id}.{table_id}`
        SET status = 'completed',
            end_time = CURRENT_TIMESTAMP()
        WHERE run_id = '{run_id}'
        """
        
        query_job = client.query(update_query)
        query_job.result()
        
        return json.dumps({
            'status': 'success',
            'run_id': run_id,
            'message': 'Orchestrator completed successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error in orchestrator: {str(e)}")
        return json.dumps({'error': str(e)}), 500