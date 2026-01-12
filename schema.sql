CREATE TABLE campaign_daily_stats (
    event_date DATE NOT NULL,
    campaign_id BIGINT NOT NULL,
    impressions BIGINT,
    clicks BIGINT,
    total_cost DOUBLE PRECISION,
    ctr DOUBLE PRECISION
);