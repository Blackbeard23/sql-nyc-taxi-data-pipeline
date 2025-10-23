CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-01.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-02.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-03.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-04.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-05.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-06.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-07.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-08.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-09.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-10.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-11.csv');

CALL bronze.incremental_load('C:/Users/ajibade.adeleke/Desktop/nyc taxi sql etl/data/yellow_tripdata_2024-12.csv');

SELECT * FROM meta.load_metadata lm;


