import functions_framework
from google.cloud import bigquery
from datetime import datetime
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def orchestrator(request):
    project_id = os.environ.get('GCP_PROJECT_ID', 'biz-hunter-oauth')
    dataset_id = 'business_data'
    table_id = 'scraper_runs'
    
    client = bigquery.Client(project=project_id)
    request_json = request.get_json(silent=True)
    
    trigger_source = 'scheduler'
    if request_json and 'trigger_type' in request_json:
        if isinstance(request_json['trigger_type'], dict):
            trigger_source = request_json['trigger_type'].get('type', 'scheduler')
        else:
            trigger_source = str(request_json['trigger_type'])
    
    now = datetime.utcnow()
    run_id = 'run_' + now.strftime('%Y%m%d_%H%M%S')
    
    scraper_run = {
        'run_id': run_id,
        'source': 'orchestrator',
        'start_time': now.isoformat(),
        'end_time': now.isoformat(),
        'status': 'completed',
        'trigger_type': trigger_source,
        'scraper_version': '1.0.2',
        'ingestion_date': now.date().isoformat(),
        'total_listings_found': 0,
        'new_listings': 0,
        'updated_listings': 0
    }
    
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        errors = client.insert_rows_json(table, [scraper_run])
        
        if errors:
            logger.error('BigQuery insert errors: %s', errors)
            return json.dumps({'error': 'Failed to insert run record', 'details': str(errors)}), 500
        
        logger.info('Successfully created scraper run: %s', run_id)
        
        return json.dumps({
            'status': 'success',
            'run_id': run_id,
            'message': 'Orchestrator completed successfully'
        }), 200
        
    except Exception as e:
        logger.error('Error in orchestrator: %s', str(e))
        return json.dumps({'error': str(e)}), 500