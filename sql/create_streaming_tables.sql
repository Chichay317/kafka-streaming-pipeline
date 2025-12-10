CREATE TABLE IF NOT EXISTS events_per_second (
    ts TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
    events_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_rolling_avg (
    sensor_id TEXT NOT NULL,
    ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    rolling_avg DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (sensor_id, ts)
);
