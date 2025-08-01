import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging

class BigQueryHandler:
    def __init__(self, project_id: str, dataset_name: str):
        if not project_id or not dataset_name:
            raise ValueError("Project ID and Dataset Name must be provided.")
        
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.dataset_id = f"{self.project_id}.{self.dataset_name}"
        self.logger = logging.getLogger("BigQueryHandler")
        self._create_dataset_if_not_exists()

    def _get_schema(self):
        """Defines the schema for the business listings tables."""
        return [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("listing_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("cash_flow", "FLOAT"),
            bigquery.SchemaField("multiple", "FLOAT"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("industry", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("seller_financing", "BOOLEAN"),
            bigquery.SchemaField("established_year", "INTEGER"),
            bigquery.SchemaField("employees", "INTEGER"),
            bigquery.SchemaField("ebitda", "FLOAT"),
            bigquery.SchemaField("inventory_value", "FLOAT"),
            bigquery.SchemaField("ffe_value", "FLOAT"),
            bigquery.SchemaField("reason_for_selling", "STRING"),
            bigquery.SchemaField("website", "STRING"),
            bigquery.SchemaField("monthly_traffic", "STRING"),
            bigquery.SchemaField("is_amazon_fba", "BOOLEAN"),
            bigquery.SchemaField("amazon_business_type", "STRING"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("enhanced_at", "TIMESTAMP"),
        ]
    
    def _get_logs_schema(self):
        """Defines the schema for the scraping logs table."""
        return [
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("site_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("start_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("end_time", "TIMESTAMP"),
            bigquery.SchemaField("duration_seconds", "FLOAT"),
            bigquery.SchemaField("total_listings_found", "INTEGER"),
            bigquery.SchemaField("existing_listings", "INTEGER"),
            bigquery.SchemaField("new_listings", "INTEGER"),
            bigquery.SchemaField("successful_scrapes", "INTEGER"),
            bigquery.SchemaField("failed_scrapes", "INTEGER"),
            bigquery.SchemaField("error_count", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),  # 'running', 'completed', 'failed'
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("api_calls_made", "INTEGER"),
            bigquery.SchemaField("api_credits_saved", "INTEGER"),
        ]

    def _create_dataset_if_not_exists(self):
        """Creates the BigQuery dataset if it doesn't already exist."""
        try:
            self.client.get_dataset(self.dataset_id)
            self.logger.info(f"Dataset {self.dataset_id} already exists.")
        except NotFound:
            self.logger.info(f"Dataset {self.dataset_id} not found, creating it.")
            dataset = bigquery.Dataset(self.dataset_id)
            dataset.location = "US"  # You can change this to your desired location
            self.client.create_dataset(dataset, timeout=30)
            self.logger.info(f"Successfully created dataset {self.dataset_id}.")

    def create_table_if_not_exists(self, site_name: str):
        """Creates a table for a specific site if it doesn't already exist."""
        table_name = f"businesses_{site_name.lower()}"
        table_id = f"{self.dataset_id}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            self.logger.info(f"Table {table_id} already exists.")
        except NotFound:
            self.logger.info(f"Table {table_id} not found, creating it.")
            schema = self._get_schema()
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table, timeout=30)
            self.logger.info(f"Successfully created table {table_id}.")
        return table_id

    def insert_rows(self, site_name: str, rows: list):
        """Inserts rows into the specified site's table."""
        if not rows:
            return

        table_name = f"businesses_{site_name.lower()}"
        table_id = f"{self.dataset_id}.{table_name}"
        
        # Ensure the table exists before trying to insert
        self.create_table_if_not_exists(site_name)
        
        errors = self.client.insert_rows_json(table_id, rows)
        if not errors:
            self.logger.info(f"Successfully inserted {len(rows)} rows into {table_id}.")
        else:
            self.logger.error(f"Encountered errors while inserting rows into {table_id}: {errors}")
    
    def get_existing_urls(self, site_name: str, urls: list) -> set:
        """Check which URLs already exist in the database to avoid duplicate scraping."""
        if not urls:
            return set()
        
        table_name = f"businesses_{site_name.lower()}"
        table_id = f"{self.dataset_id}.{table_name}"
        
        # Check if table exists first
        try:
            self.client.get_table(table_id)
        except NotFound:
            # Table doesn't exist, so no URLs exist
            return set()
        
        # Build query to check for existing URLs
        # Process URLs in batches to avoid query length limits
        existing_urls = set()
        batch_size = 500
        
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            
            # Create a parameterized query for safety
            query = f"""
            SELECT DISTINCT listing_url
            FROM `{table_id}`
            WHERE listing_url IN UNNEST(@urls)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("urls", "STRING", batch)
                ]
            )
            
            try:
                query_job = self.client.query(query, job_config=job_config)
                results = query_job.result()
                
                for row in results:
                    existing_urls.add(row.listing_url)
                    
            except Exception as e:
                self.logger.error(f"Error checking existing URLs: {e}")
        
        self.logger.info(f"Found {len(existing_urls)} existing URLs out of {len(urls)} checked")
        return existing_urls
    
    def create_logs_table_if_not_exists(self):
        """Creates the scraping logs table if it doesn't already exist."""
        table_name = "scraping_logs"
        table_id = f"{self.dataset_id}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            self.logger.info(f"Table {table_id} already exists.")
        except NotFound:
            self.logger.info(f"Table {table_id} not found, creating it.")
            schema = self._get_logs_schema()
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table, timeout=30)
            self.logger.info(f"Successfully created table {table_id}.")
        return table_id
    
    def log_scraping_run(self, log_data: dict):
        """Log a scraping run to the scraping_logs table."""
        table_name = "scraping_logs"
        table_id = f"{self.dataset_id}.{table_name}"
        
        # Ensure the logs table exists
        self.create_logs_table_if_not_exists()
        
        # Insert the log record
        errors = self.client.insert_rows_json(table_id, [log_data])
        if not errors:
            self.logger.info(f"Successfully logged scraping run {log_data.get('run_id')} to {table_id}.")
        else:
            self.logger.error(f"Error logging scraping run: {errors}")
    
    def update_scraping_log(self, run_id: str, updates: dict):
        """Update an existing scraping log entry."""
        table_name = "scraping_logs"
        table_id = f"{self.dataset_id}.{table_name}"
        
        # Build the update query
        set_clauses = []
        for key, value in updates.items():
            if isinstance(value, str):
                set_clauses.append(f"{key} = '{value}'")
            elif value is None:
                set_clauses.append(f"{key} = NULL")
            else:
                set_clauses.append(f"{key} = {value}")
        
        query = f"""
        UPDATE `{table_id}`
        SET {', '.join(set_clauses)}
        WHERE run_id = '{run_id}'
        """
        
        try:
            query_job = self.client.query(query)
            query_job.result()
            self.logger.info(f"Successfully updated scraping log for run_id: {run_id}")
        except Exception as e:
            self.logger.error(f"Error updating scraping log: {e}")

# Example of how to get the handler
_handler = None
def get_bigquery_handler():
    global _handler
    if _handler is None:
        project_id = os.getenv("GCP_PROJECT_ID", "tetrahedron-366117")
        dataset_name = os.getenv("BQ_DATASET_NAME", "business_listings")
        _handler = BigQueryHandler(project_id, dataset_name)
    return _handler
