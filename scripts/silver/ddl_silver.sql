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
	minute_duration integer,
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
)PARTITION BY RANGE (tpep_pickup_datetime);

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_01
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_02
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_03
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_04
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_05
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_06
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_07
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_08
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_09
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_10
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_11
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');

CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_12
  PARTITION OF silver.yellow_taxi
  FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

