"""
Orchestrator Cloud Function that triggers all scrapers
"""

import os
import uuid
import json
import functions_framework
from datetime import datetime, timedelta, date
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from bigquery_writer import BigQueryWriter
import concurrent.futures


bq_writer = BigQueryWriter()
PROJECT_ID = os.environ.get('GCP_PROJECT')
LOCATION = os.environ.get('FUNCTION_REGION', 'us-central1')
QUEUE_NAME = 'scraper-queue'

# List of scraper functions to trigger
SCRAPERS = [
    {
        'name': 'scrape-empire-flippers',
        'source': 'Empire Flippers',
        'url': f'https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/scrape-empire-flippers'
    },
    {
        'name': 'scrape-bizquest',
        'source': 'BizQuest',
        'url': f'https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/scrape-bizquest'
    },
    {
        'name': 'scrape-quietlight',
        'source': 'Quiet Light',
        'url': f'https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/scrape-quietlight'
    },
    {
        'name': 'scrape-bizbuysell',
        'source': 'BizBuySell',
        'url': f'https://{LOCATION}-{PROJECT_ID}.cloudfunctions.net/scrape-bizbuysell'
    }
]


def create_task(scraper_config, delay_seconds=0):
    """Create a Cloud Task to trigger a scraper function"""
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
    
    task = {
        'http_request': {
            'http_method': tasks_v2.HttpMethod.POST,
            'url': scraper_config['url'],
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'triggered_by': 'orchestrator',
                'source': scraper_config['source']
            }).encode()
        }
    }
    
    if delay_seconds > 0:
        d = datetime.utcnow() + timedelta(seconds=delay_seconds)
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(d)
        task['schedule_time'] = timestamp
    
    response = client.create_task(request={'parent': parent, 'task': task})
    return response.name


@functions_framework.http
def main(request):
    """HTTP Cloud Function entry point"""
    
    # Check if this is a scheduled run
    is_scheduled = request.headers.get('X-CloudScheduler', False)
    
    orchestration_id = str(uuid.uuid4())
    
    # Create orchestration record for BigQuery
    orchestration_data = {
        'run_id': orchestration_id,
        'source': 'Orchestrator',
        'start_time': datetime.utcnow(),
        'status': 'running',
        'trigger_type': {'is_scheduled': bool(is_scheduled)},
        'ingestion_date': date.today()
    }
    
    try:
        triggered_scrapers = []
        errors = []
        
        # Trigger each scraper with a delay to avoid rate limits
        for i, scraper in enumerate(SCRAPERS):
            try:
                # Add delay between scrapers (30 seconds)
                delay = i * 30
                task_name = create_task(scraper, delay_seconds=delay)
                
                triggered_scrapers.append({
                    'name': scraper['name'],
                    'source': scraper['source'],
                    'task_name': task_name,
                    'triggered_at': datetime.utcnow()
                })
                
            except Exception as e:
                errors.append({
                    'scraper': scraper['name'],
                    'error': str(e)
                })
        
        # Update orchestration record in BigQuery
        orchestration_data.update({
            'end_time': datetime.utcnow(),
            'status': 'completed' if not errors else 'partial',
            'total_listings_found': len(triggered_scrapers),
            'new_listings': len(triggered_scrapers),
            'updated_listings': 0,
            'error_message': str(errors) if errors else None
        })
        bq_writer.write_scraper_run(orchestration_data)
        
        return {
            'success': True,
            'orchestration_id': orchestration_id,
            'scrapers_triggered': len(triggered_scrapers),
            'errors': errors
        }, 200
        
    except Exception as e:
        # Update orchestration record with error in BigQuery
        orchestration_data.update({
            'end_time': datetime.utcnow(),
            'status': 'failed',
            'error_message': str(e)
        })
        bq_writer.write_scraper_run(orchestration_data)
        
        return {
            'success': False,
            'orchestration_id': orchestration_id,
            'error': str(e)
        }, 500