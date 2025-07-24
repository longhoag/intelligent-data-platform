-- Initialize database schema for intelligent data platform

-- Create schema for feature store
CREATE SCHEMA IF NOT EXISTS feature_store;

-- Create table for feature metadata
CREATE TABLE IF NOT EXISTS feature_store.feature_metadata (
    id SERIAL PRIMARY KEY,
    feature_name VARCHAR(255) NOT NULL UNIQUE,
    feature_type VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create table for feature values (time series)
CREATE TABLE IF NOT EXISTS feature_store.feature_values (
    id SERIAL PRIMARY KEY,
    feature_name VARCHAR(255) NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    feature_value NUMERIC,
    string_value TEXT,
    json_value JSONB,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_feature_metadata 
        FOREIGN KEY (feature_name) 
        REFERENCES feature_store.feature_metadata(feature_name)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_feature_values_name_time 
    ON feature_store.feature_values(feature_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_feature_values_entity_time 
    ON feature_store.feature_values(entity_id, timestamp DESC);

-- Create schema for streaming metadata
CREATE SCHEMA IF NOT EXISTS streaming;

-- Create table for stream processing jobs
CREATE TABLE IF NOT EXISTS streaming.processing_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL UNIQUE,
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'CREATED',
    config JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_run_at TIMESTAMP,
    next_run_at TIMESTAMP
);

-- Create table for pipeline metrics
CREATE TABLE IF NOT EXISTS streaming.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value NUMERIC,
    timestamp TIMESTAMP NOT NULL,
    CONSTRAINT fk_processing_jobs 
        FOREIGN KEY (job_name) 
        REFERENCES streaming.processing_jobs(job_name)
);

-- Create table for alerts and notifications
CREATE TABLE IF NOT EXISTS streaming.alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT,
    severity VARCHAR(20) DEFAULT 'INFO',
    source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged_at TIMESTAMP,
    resolved_at TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_job_time 
    ON streaming.pipeline_metrics(job_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_type_time 
    ON streaming.alerts(alert_type, created_at DESC);

-- Insert initial data
INSERT INTO feature_store.feature_metadata (feature_name, feature_type, description) VALUES
    ('price_volatility', 'NUMERIC', 'Rolling price volatility calculation'),
    ('volume_moving_avg', 'NUMERIC', 'Moving average of trading volume'),
    ('momentum_indicator', 'NUMERIC', 'Price momentum over time window'),
    ('rsi', 'NUMERIC', 'Relative Strength Index'),
    ('bollinger_position', 'NUMERIC', 'Position within Bollinger Bands')
ON CONFLICT (feature_name) DO NOTHING;

INSERT INTO streaming.processing_jobs (job_name, job_type, status, config) VALUES
    ('market_data_processor', 'STREAM_CONSUMER', 'ACTIVE', '{"topics": ["market-data"], "parallelism": 3}'),
    ('transaction_processor', 'STREAM_CONSUMER', 'ACTIVE', '{"topics": ["transactions"], "parallelism": 2}'),
    ('feature_computation', 'STREAM_PROCESSOR', 'ACTIVE', '{"window_size": 300, "slide_interval": 60}'),
    ('anomaly_detection', 'ML_PIPELINE', 'ACTIVE', '{"model_type": "isolation_forest", "threshold": 0.5}')
ON CONFLICT (job_name) DO NOTHING;
