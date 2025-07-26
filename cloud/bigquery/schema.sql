-- BigQuery Schema for Business Listings Scraper
-- This schema normalizes data from multiple business marketplaces

-- Main business listings table
CREATE TABLE IF NOT EXISTS `biz-hunter-oauth.business_data.listings` (
  -- Primary identifiers
  listing_id STRING NOT NULL,
  source STRING NOT NULL,
  source_url STRING,
  
  -- Business details
  title STRING,
  description STRING,
  business_type STRING,
  category STRING,
  
  -- Financial information (normalized)
  asking_price_raw STRING,
  asking_price_numeric FLOAT64,
  asking_price_currency STRING DEFAULT 'USD',
  
  revenue_raw STRING,
  revenue_numeric FLOAT64,
  revenue_currency STRING DEFAULT 'USD',
  revenue_period STRING, -- 'annual', 'monthly', etc.
  
  profit_raw STRING,
  profit_numeric FLOAT64,
  profit_currency STRING DEFAULT 'USD',
  profit_period STRING, -- 'annual', 'monthly', etc.
  
  cash_flow_raw STRING,
  cash_flow_numeric FLOAT64,
  cash_flow_currency STRING DEFAULT 'USD',
  cash_flow_period STRING,
  
  -- Calculated metrics
  price_to_revenue_multiple FLOAT64,
  price_to_profit_multiple FLOAT64,
  profit_margin_percent FLOAT64,
  
  -- Location information
  location_raw STRING,
  country STRING,
  state STRING,
  city STRING,
  
  -- Business characteristics
  established_year INT64,
  established_date DATE,
  business_age_years INT64,
  
  -- Niches and categories (repeated fields for multiple values)
  niches ARRAY<STRING>,
  tags ARRAY<STRING>,
  
  -- Operational details
  monetization_method STRING,
  traffic_source STRING,
  business_model STRING,
  
  -- Data quality and metadata
  is_active BOOLEAN DEFAULT TRUE,
  data_completeness_score FLOAT64, -- 0-1 score based on filled fields
  last_verified TIMESTAMP,
  
  -- Timestamps
  first_seen TIMESTAMP,
  last_updated TIMESTAMP,
  scraped_at TIMESTAMP,
  
  -- Partitioning and clustering
  ingestion_date DATE,
  
  -- Raw data preservation
  raw_data JSON
)
PARTITION BY ingestion_date
CLUSTER BY source, is_active, asking_price_numeric;

-- Scraper run tracking table
CREATE TABLE IF NOT EXISTS `biz-hunter-oauth.business_data.scraper_runs` (
  run_id STRING NOT NULL,
  source STRING NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING, -- 'running', 'success', 'error', 'partial'
  
  -- Run statistics
  total_listings_found INT64,
  new_listings INT64,
  updated_listings INT64,
  deactivated_listings INT64,
  
  -- Error tracking
  error_message STRING,
  error_count INT64,
  
  -- Performance metrics
  pages_scraped INT64,
  requests_made INT64,
  avg_response_time_ms FLOAT64,
  
  -- Configuration
  trigger_type STRING, -- 'scheduled', 'manual', 'api'
  scraper_version STRING,
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY source, status;

-- Data quality metrics table
CREATE TABLE IF NOT EXISTS `biz-hunter-oauth.business_data.data_quality` (
  check_id STRING NOT NULL,
  table_name STRING NOT NULL,
  check_type STRING NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'validity'
  check_name STRING NOT NULL,
  
  -- Results
  total_records INT64,
  passed_records INT64,
  failed_records INT64,
  pass_rate FLOAT64,
  
  -- Details
  check_query STRING,
  failure_examples ARRAY<STRING>,
  
  -- Timestamps
  check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE
)
PARTITION BY ingestion_date
CLUSTER BY table_name, check_type;

-- Source mapping table for normalization
CREATE TABLE IF NOT EXISTS `biz-hunter-oauth.business_data.source_mappings` (
  source STRING NOT NULL,
  field_name STRING NOT NULL,
  raw_value STRING NOT NULL,
  normalized_value STRING NOT NULL,
  mapping_type STRING NOT NULL, -- 'category', 'location', 'business_type', etc.
  confidence_score FLOAT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY source, mapping_type;

-- Business trends aggregation table (materialized view)
CREATE TABLE IF NOT EXISTS `biz-hunter-oauth.business_data.daily_trends` (
  date DATE NOT NULL,
  source STRING NOT NULL,
  
  -- Listing counts
  total_active_listings INT64,
  new_listings_today INT64,
  updated_listings_today INT64,
  
  -- Financial metrics
  avg_asking_price FLOAT64,
  median_asking_price FLOAT64,
  avg_revenue FLOAT64,
  median_revenue FLOAT64,
  avg_profit_margin FLOAT64,
  
  -- Price ranges
  listings_under_100k INT64,
  listings_100k_to_500k INT64,
  listings_500k_to_1m INT64,
  listings_1m_to_5m INT64,
  listings_over_5m INT64,
  
  -- Top categories
  top_categories ARRAY<STRUCT<category STRING, count INT64>>,
  top_niches ARRAY<STRUCT<niche STRING, count INT64>>,
  
  -- Data quality
  avg_completeness_score FLOAT64,
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY date
CLUSTER BY source;

-- Views for easy querying

-- Active listings view
CREATE OR REPLACE VIEW `biz-hunter-oauth.business_data.active_listings` AS
SELECT 
  listing_id,
  source,
  title,
  asking_price_numeric,
  revenue_numeric,
  profit_numeric,
  location_raw,
  niches,
  price_to_profit_multiple,
  profit_margin_percent,
  last_updated,
  source_url
FROM `biz-hunter-oauth.business_data.listings`
WHERE is_active = TRUE
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- Summary statistics view
CREATE OR REPLACE VIEW `biz-hunter-oauth.business_data.summary_stats` AS
SELECT 
  source,
  COUNT(*) as total_listings,
  COUNT(CASE WHEN is_active THEN 1 END) as active_listings,
  AVG(asking_price_numeric) as avg_price,
  APPROX_QUANTILES(asking_price_numeric, 2)[OFFSET(1)] as median_price,
  AVG(revenue_numeric) as avg_revenue,
  AVG(profit_numeric) as avg_profit,
  AVG(data_completeness_score) as avg_completeness,
  MAX(last_updated) as last_update
FROM `biz-hunter-oauth.business_data.listings`
WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY source;

-- Data quality dashboard view
CREATE OR REPLACE VIEW `biz-hunter-oauth.business_data.quality_dashboard` AS
SELECT 
  source,
  COUNT(*) as total_records,
  AVG(data_completeness_score) as avg_completeness,
  COUNT(CASE WHEN asking_price_numeric IS NOT NULL THEN 1 END) / COUNT(*) as price_completeness,
  COUNT(CASE WHEN revenue_numeric IS NOT NULL THEN 1 END) / COUNT(*) as revenue_completeness,
  COUNT(CASE WHEN profit_numeric IS NOT NULL THEN 1 END) / COUNT(*) as profit_completeness,
  COUNT(CASE WHEN location_raw IS NOT NULL THEN 1 END) / COUNT(*) as location_completeness,
  COUNT(CASE WHEN array_length(niches) > 0 THEN 1 END) / COUNT(*) as niche_completeness
FROM `biz-hunter-oauth.business_data.listings`
WHERE is_active = TRUE
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY source;