CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.yellow_taxi_raw;
CREATE TABLE bronze.yellow_taxi_raw (
	vendorid integer,
	tpep_pickup_datetime timestamp,
	tpep_dropoff_datetime timestamp,
	passenger_count text,
	trip_distance NUMERIC,
	ratecodeid text,
	store_and_fwd_flag text,
	pulocationid integer,
	dolocationid integer,
	payment_type integer,
	fare_amount NUMERIC,
	extra NUMERIC,
	mta_tax NUMERIC,
	tip_amount NUMERIC,
	tolls_amount NUMERIC,
	improvement_surcharge NUMERIC,
	total_amount NUMERIC,
	congestion_surcharge NUMERIC,
	airport_fee NUMERIC
);


CREATE SCHEMA IF NOT EXISTS meta;

DROP TYPE IF EXISTS meta.status_enum CASCADE;
CREATE TYPE meta.status_enum AS ENUM ('success','failed', 'skipped');

DROP TABLE IF EXISTS meta.load_metadata;
CREATE TABLE meta.load_metadata (
	file_name text,
	no_of_rows_inserted int,
	last_dropoff_datetime timestamp,
	status meta.status_enum,
	load_timestamp timestamp
);

DROP TABLE IF EXISTS meta.invalid_records;
CREATE TABLE meta.invalid_records (LIKE bronze.yellow_taxi_raw,
	PRIMARY KEY (vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
							  trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
							  payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
							  improvement_surcharge, total_amount, congestion_surcharge, airport_fee)
);