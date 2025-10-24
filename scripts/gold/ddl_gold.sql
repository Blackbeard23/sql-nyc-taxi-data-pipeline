CREATE SCHEMA IF NOT EXISTS gold;

CREATE VIEW gold.vendor_metrics AS 
SELECT 
	v.vendor,
	COUNT(*) AS total_trips,
	SUM(yt.total_amount) AS total_revenue,
	ROUND(AVG(yt.minute_duration),2) AS avg_minute_duration
FROM silver.yellow_taxi yt
LEFT JOIN silver.vendor v ON yt.vendorid = v.vendorid
GROUP BY v.vendor
ORDER BY total_revenue DESC;


CREATE VIEW gold.monthly_metrics AS
SELECT 
	TO_CHAR(DATE_TRUNC('month', tpep_pickup_datetime), 'Month') AS montht,
	COUNT(*) AS total_rides,
	ROUND(AVG(minute_duration),2) AS avg_minute_duration,
	ROUND(AVG(trip_distance),2) AS avg_distance_miles
FROM silver.yellow_taxi
GROUP BY TO_CHAR(DATE_TRUNC('month', tpep_pickup_datetime), 'Month')
ORDER BY total_rides DESC;


/* To answer:
 - does payment type affects ride distance and duration
*/
CREATE VIEW gold.payment_metrics AS
SELECT 
	pt.payment_type,
	COUNT(*) AS total_trip_by_payment,
	ROUND(AVG(yt.trip_distance),2) AS avg_trip_distance_mile,
	ROUND(AVG(yt.minute_duration),2) AS avg_duration_minute
FROM silver.yellow_taxi yt
LEFT JOIN silver.payment_type pt ON yt.payment_type = pt.payment_type_id
GROUP BY pt.payment_type
ORDER BY total_trip_by_payment DESC;