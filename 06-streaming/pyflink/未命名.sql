CREATE TABLE IF NOT EXISTS processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);


CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP(3),
    test_data INT,
    num_hits BIGINT,
    PRIMARY KEY (event_hour, test_data)
);



select * from postgres.public.processed_events

select * from processed_events_aggregated
order by event_hour desc

drop table postgres.public.processed_events

drop table processed_events_aggregated


