DROP TABLE processed_events;

DROP TABLE processed_events_aggregated;

CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);


CREATE TABLE IF NOT EXISTS taxi_rides (
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance NUMERIC,
            tip_amount NUMERIC
);

select * from taxi_rides

CREATE TABLE IF NOT EXISTS taxi_sessions (
	PULocationID INTEGER,
	DOLocationID INTEGER,
	session_start TIMESTAMP,
	session_end TIMESTAMP,
)
PULocationID,
    DOLocationID,
    SESSION_START(dropoff_time) AS session_start,
    SESSION_END(dropoff_time) AS session_end,
    COUNT(*) AS trip_count