INSERT INTO vendor (vendorid, vendor)
SELECT
	vendorid,
	CASE
		WHEN vendorid = 1 THEN 'Creative Mobile Technologies, LLC'
		WHEN vendorid = 2 THEN 'Curb Mobility, LLC'
		WHEN vendorid = 6 THEN 'Myle Technologies Inc'
		WHEN vendorid = 7 THEN 'Helix'
	END AS vendor
FROM (SELECT DISTINCT vendorid FROM bronze.yellow_taxi_raw);


INSERT INTO ratecode (ratecodeid, rate)
VALUES (1, 'Standard rate'),
		(2, 'JFK'),
		(3, 'Newark'),
		(4, 'Nassau or Westchester'),
		(5, 'Negotiated fare'),
		(6, 'Group ride'),
		(99, 'Unknown');


INSERT INTO payment_type (payment_type_id, payment_type)
VALUES (0, 'Flex Fare trip'),
		(1, 'Credit card'),
		(2, 'Cash'),
		(3, 'No charge'),
		(4, 'Dispute'),
		(5, 'Unknown'),
		(6, 'Voided trip');


INSERT INTO silver.yellow_taxi (
  vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, minute_duration, passenger_count,
  trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
  payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
  improvement_surcharge, total_amount, congestion_surcharge, airport_fee
)
WITH dedup AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
							  trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
							  payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
							  improvement_surcharge, total_amount, congestion_surcharge, airport_fee
							  ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime) AS row_dedup
 	FROM bronze.yellow_taxi_raw
 )
SELECT
	vendorid,
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	CAST(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 AS integer) AS minute_duration,
	passenger_count,
	trip_distance,
	CAST(ratecodeid AS NUMERIC)::INTEGER AS ratecodeid,
	store_and_fwd_flag,
	pulocationid,
	dolocationid,
	CAST(payment_type AS integer) AS payment_type,
	fare_amount,
	extra,
	mta_tax,
	tip_amount,
	tolls_amount,
	improvement_surcharge,
	total_amount,
	congestion_surcharge,
	airport_fee
FROM dedup
WHERE row_dedup = 1 
	AND (tpep_pickup_datetime >= DATE '2024-01-01'
  AND tpep_pickup_datetime <  DATE '2025-01-01');

