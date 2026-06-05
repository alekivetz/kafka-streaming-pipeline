/*
=============================================================
Create Database and Tables
=============================================================
The script is responsible for creating the following tables:
    - minute_metrics: a table that stores revenue metrics per minute
    - item_counts: a table that stores item counts by category
    - recent_payments: a table that stores recent payments
    - server_metrics: a table that stores server metrics

*/

-- Orders and revenue metrics per minute
CREATE TABLE IF NOT EXISTS minute_metrics (
    minute_timestamp        TIMESTAMP UNIQUE,
    order_count             INT,
    total_revenue           NUMERIC
);

-- Item counts
CREATE TABLE IF NOT EXISTS item_counts (
    item_name               VARCHAR(50) UNIQUE,
    category                VARCHAR(50),
    count                   INT
);

-- Payment logs
CREATE TABLE IF NOT EXISTS recent_payments (
    paid_at                     TIMESTAMP,
    payment_method              VARCHAR(10),
    total_amount                NUMERIC,
    table_number                INT,
    table_size                  INT,
    table_duration_minutes      FLOAT
);

-- Server metrics
CREATE TABLE IF NOT EXISTS server_metrics (
    server_name               VARCHAR(50) UNIQUE,
    table_count               INT,
    total_covers              INT,
    total_revenue             NUMERIC,
    total_duration_minutes    FLOAT
);

-- Alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id                  VARCHAR(50) UNIQUE,
    alert_type                VARCHAR(50),
    message                   VARCHAR(255),
    triggered_at              TIMESTAMP
);
