CREATE OR REPLACE PROCEDURE bronze.incremental_load(file_path text)
LANGUAGE plpgsql
AS $$
DECLARE
  v_wm timestamp;   -- metadata lastest datetime
  v_rows int;         -- number of new file rows
  v_last_in timestamp;   -- max dropoff in the incoming file
  fle_name text;
  month_start timestamp;
  month_end timestamp;
  upload_status meta.status_enum;
BEGIN
  -- 1) staging table shaped like Bronze (LIKE to copy table structure)
  CREATE TEMP TABLE IF NOT EXISTS stage_yellow (LIKE bronze.yellow_taxi_raw);
  TRUNCATE stage_yellow;

  -- 2) load file into staging (server-side COPY)
  EXECUTE format($f$
    COPY stage_yellow FROM %L WITH (FORMAT csv, HEADER true)
  $f$, file_path);

  -- 3) compute stats
--  SELECT MAX(tpep_dropoff_datetime) INTO v_last_in FROM stage_yellow;

  SELECT COALESCE(MAX(last_dropoff_datetime), TIMESTAMP '2023-12-31 00:00:00')
  INTO v_wm
  FROM meta.load_metadata;
  
  SELECT SPLIT_PART(file_path, '/', 7) INTO fle_name;

  SELECT TO_DATE((REGEXP_MATCHES(fle_name, '\d{4}-\d{2}'))[1],'YYYY-MM') INTO month_start;

  SELECT month_start + INTERVAL '1 month' INTO month_end;
  
  SELECT COUNT(*) INTO v_rows FROM stage_yellow
  WHERE tpep_pickup_datetime >= month_start AND tpep_pickup_datetime < month_end;

  SELECT CASE WHEN v_rows > 0 THEN 'success' ELSE 'skipped' END AS status INTO upload_status;

  -- 4) incremental insert into Bronze
  INSERT INTO bronze.yellow_taxi_raw (
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  )
  SELECT
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  FROM stage_yellow
  WHERE tpep_pickup_datetime >= month_start AND tpep_pickup_datetime < month_end;

  -- out of range records into meta.invalid_records
  INSERT INTO meta.invalid_records (
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  )
  SELECT
    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
    trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid,
    payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
  FROM stage_yellow
  WHERE NOT (tpep_pickup_datetime >= month_start AND tpep_pickup_datetime < month_end)
  ON CONFLICT(vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
				trip_distance, pulocationid, dolocationid, total_amount) DO NOTHING;

  SELECT MAX(tpep_pickup_datetime) INTO v_last_in FROM bronze.yellow_taxi_raw;

  -- 5) log success in metadata
  INSERT INTO meta.load_metadata(file_name, no_of_rows_inserted, last_dropoff_datetime, status, load_timestamp)
  VALUES (fle_name, v_rows, v_last_in, upload_status, now());

EXCEPTION WHEN OTHERS THEN
  -- log failure; keep as much info as we have
  INSERT INTO meta.load_metadata(file_name, no_of_rows_inserted, last_dropoff_datetime, status, load_timestamp)
  VALUES (file_path, COALESCE(v_rows, 0), v_last_in, 'failed', now());
  RAISE;
END;
$$;

--DROP PROCEDURE bronze.incremental_load

	