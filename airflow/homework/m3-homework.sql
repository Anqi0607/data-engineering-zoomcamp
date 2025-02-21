--big query setup

--create an external table using the parquet data in bucket
create or replace external table `trips_data_all.external_yellow_2024_data`
options (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-terraform-02052025-bucket/Yellow-2024/yellow_tripdata_*.parquet']
);


--create a regular table in the dataset from the external table; no partioning or clustering
create or replace table `trips_data_all.yellow_2024_data` as
select * from trips_data_all.external_yellow_2024_data;

--Question 1: What is count of records for the 2024 Yellow Taxi Data?
--answer: 20,332,093

--Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
--Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table
select count(distinct PULocationID)
from trips_data_all.external_yellow_2024_data;

select count(distinct PULocationID)
from trips_data_all.yellow_2024_data;

--Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
--Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

--will scan 155.12mb data
select PULocationID
from `trips_data_all.yellow_2024_data`;

--will scan 310.24mb data
select PULocationID, DOLocationID
from `trips_data_all.yellow_2024_data`;


--Question 4: How many records have a fare_amount of 0?
--Answer: 8,333
select count(*)
from `trips_data_all.yellow_2024_data`
where fare_amount = 0;

--Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
--Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID
create or replace table trips_data_all.yellow_2024_data_partitioned_clustered
partition by date(tpep_dropoff_datetime)
cluster by VendorID as
select * from `trips_data_all.yellow_2024_data`;


--Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
--Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
--Choose the answer which most closely matches.
--Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

--will scan 310.24mb data
select distinct VendorID
from `trips_data_all.yellow_2024_data`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';

--will scan 26.84mb data
select distinct VendorID
from `trips_data_all.yellow_2024_data_partitioned_clustered`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';

--Questions 7: Where is the data stored in the External Table you created?
--Answer: GCP Bucket

--Question 8:It is best practice in Big Query to always cluster your data:
--Answer: No

--Question 9: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
--Answer: 0b; because it's already precomputed and aggregated along with metadata.
create or replace materialized view `trips_data_all.view_yellow_2024` as
select * from `trips_data_all.yellow_2024_data`;


select count(*)
from `trips_data_all.yellow_2024_data`;

select count(*)
from `trips_data_all.view_yellow_2024`;
