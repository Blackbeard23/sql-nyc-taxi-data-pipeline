CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.vendor;
CREATE TABLE silver.vendor (
	vendorid integer,
	vendor text,
	PRIMARY KEY (vendorid)
);

DROP TABLE IF EXISTS silver.ratecode;
CREATE TABLE silver.ratecode (
	ratecodeid integer,
	rate text,
	PRIMARY KEY (ratecodeid)
);

DROP TABLE IF EXISTS silver.payment_type;
CREATE TABLE silver.payment_type (
	payment_type_id integer,
	payment_type text,
	PRIMARY KEY (payment_type_id)
);

DROP TABLE IF EXISTS silver.yellow_taxi;
CREATE TABLE silver.yellow_taxi (
	vendorid integer REFERENCES silver.vendor (vendorid),
	tpep_pickup_datetime timestamp,
	tpep_dropoff_datetime timestamp,
	passenger_count text,
	trip_distance NUMERIC,
	ratecodeid integer REFERENCES silver.ratecode (ratecodeid),
	store_and_fwd_flag text,
	pulocationid integer,
	dolocationid integer,
	payment_type integer REFERENCES silver.payment_type (payment_type_id),
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

