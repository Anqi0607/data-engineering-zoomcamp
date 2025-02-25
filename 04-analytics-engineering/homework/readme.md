## Module 4 Homework

For this homework, you will need the following datasets:
* [Green Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)
* [Yellow Taxi dataset (2019 and 2020)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/yellow)
* [For Hire Vehicle dataset (2019)](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv)

### Question 1: Understanding dbt model resolution

Answer: 
- `select * from myproject.raw_nyc_tripdata.ext_green_taxi`

`export DBT_BIGQUERY_PROJECT=myproject` specified the value of database name to be `myproject`, instead of the default value of `dtc_zoomcamp_2025`.

`DBT_BIGQUERY_SOURCE_DATASET` isn't specified, so the default value of `raw_nyc_tripdata` will be used in the compiled query.