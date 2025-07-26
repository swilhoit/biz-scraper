"""
API Cloud Function to query business listings from BigQuery
"""

import functions_framework
from flask import jsonify
from bigquery_writer import BigQueryWriter
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bq_writer = BigQueryWriter()


@functions_framework.http
def main(request):
    """HTTP Cloud Function entry point"""
    
    # Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {
        'Access-Control-Allow-Origin': '*'
    }
    
    # Parse query parameters
    source = request.args.get('source')
    active_only = request.args.get('active', 'true').lower() == 'true'
    limit = int(request.args.get('limit', 100))
    min_price = request.args.get('min_price')
    max_price = request.args.get('max_price')
    
    try:
        # Convert price filters to numeric
        min_price_numeric = float(min_price) if min_price else None
        max_price_numeric = float(max_price) if max_price else None
        
        # Query listings from BigQuery
        result = bq_writer.query_listings(
            source=source,
            active_only=active_only,
            limit=limit,
            min_price=min_price_numeric,
            max_price=max_price_numeric
        )
        
        if not result['success']:
            raise Exception(result.get('error', 'Failed to query listings'))
        
        # Get summary statistics
        stats_result = bq_writer.get_listing_stats(source=source)
        statistics = stats_result.get('stats', {}) if stats_result['success'] else {}
        
        # Format response
        response = {
            'success': True,
            'count': result['count'],
            'listings': result['listings'],
            'statistics': format_statistics(statistics)
        }
        
        return (jsonify(response), 200, headers)
        
    except Exception as e:
        logger.error(f"API Error: {str(e)}")
        response = {
            'success': False,
            'error': str(e)
        }
        return (jsonify(response), 500, headers)


def format_statistics(stats_dict):
    """Format statistics for API response"""
    if not stats_dict:
        return {
            'total_listings': 0,
            'active_listings': 0,
            'by_source': {},
            'last_update': None
        }
    
    # Aggregate statistics across sources
    total_listings = sum(source_stats.get('total_listings', 0) for source_stats in stats_dict.values())
    active_listings = sum(source_stats.get('active_listings', 0) for source_stats in stats_dict.values())
    
    # Get the most recent update time
    last_updates = [source_stats.get('last_update') for source_stats in stats_dict.values() if source_stats.get('last_update')]
    last_update = max(last_updates) if last_updates else None
    
    # Format by_source statistics
    by_source = {}
    for source, source_stats in stats_dict.items():
        by_source[source] = source_stats.get('active_listings', 0)
    
    return {
        'total_listings': total_listings,
        'active_listings': active_listings,
        'by_source': by_source,
        'last_update': last_update,
        'avg_price': calculate_weighted_average(stats_dict, 'avg_price'),
        'median_price': calculate_weighted_average(stats_dict, 'median_price'),
        'avg_revenue': calculate_weighted_average(stats_dict, 'avg_revenue'),
        'avg_profit': calculate_weighted_average(stats_dict, 'avg_profit'),
        'avg_completeness': calculate_weighted_average(stats_dict, 'avg_completeness')
    }


def calculate_weighted_average(stats_dict, metric_key):
    """Calculate weighted average across sources"""
    total_weight = 0
    weighted_sum = 0
    
    for source_stats in stats_dict.values():
        listings_count = source_stats.get('active_listings', 0)
        metric_value = source_stats.get(metric_key)
        
        if listings_count > 0 and metric_value is not None:
            total_weight += listings_count
            weighted_sum += metric_value * listings_count
    
    return round(weighted_sum / total_weight, 2) if total_weight > 0 else None