-- Bronze layer: ingest raw ride-hailing trip CSVs from the landing volume
CREATE OR REFRESH STREAMING TABLE bronze_ride_trips
AS
SELECT *,
       _metadata.file_name AS source_file,
       current_timestamp() AS loaded_at
FROM STREAM read_files(
  '/Volumes/ride_hailing_tripe_project/default/ride_hailing_trip/',
  format => 'csv',
  header => true
);