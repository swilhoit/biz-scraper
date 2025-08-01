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

# Example of how to get the handler
_handler = None
def get_bigquery_handler():
    global _handler
    if _handler is None:
        project_id = os.getenv("GCP_PROJECT_ID", "tetrahedron-366117")
        dataset_name = os.getenv("BQ_DATASET_NAME", "business_listings")
        _handler = BigQueryHandler(project_id, dataset_name)
    return _handler
