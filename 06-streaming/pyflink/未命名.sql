CREATE TABLE IF NOT EXISTS processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);


select * from postgres.public.processed_events

drop table postgres.public.processed_events


