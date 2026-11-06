CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_trips AS
SELECT DATE_TRUNC('MONTH', trip_date)           AS trip_month,
       COUNT(*)                                 AS trips,
       SUM(distance_km)                         AS total_km,
       SUM(fare_amount - discount_amount)       AS net_revenue,
       SUM(driver_payout)                       AS driver_payout,
       SUM(fare_amount - discount_amount) - SUM(driver_payout) AS platform_earnings
FROM ride_hailing_tripe_project.trips.silver_trips_clean
GROUP BY DATE_TRUNC('MONTH', trip_date);

CREATE OR REFRESH MATERIALIZED VIEW gold_city_summary AS
SELECT region,
       city,
       COUNT(*)                                 AS trips,
       SUM(fare_amount - discount_amount)       AS net_revenue,
       AVG(fare_amount - discount_amount)       AS avg_fare_per_trip
FROM ride_hailing_tripe_project.trips.silver_trips_clean
GROUP BY region, city;

CREATE OR REFRESH MATERIALIZED VIEW gold_vehicle_summary AS
SELECT vehicle_type,
       COUNT(*)                                 AS trips,
       SUM(distance_km)                         AS total_km,
       SUM(fare_amount - discount_amount)       AS net_revenue,
       AVG(customer_rating)                     AS avg_rating
FROM ride_hailing_tripe_project.trips.silver_trips_clean
GROUP BY vehicle_type;

CREATE OR REFRESH MATERIALIZED VIEW gold_payment_summary AS
SELECT payment_method,
       COUNT(*)                                 AS trips,
       SUM(fare_amount - discount_amount)       AS net_revenue,
       SUM(CASE WHEN payment_status = 'Pending' THEN fare_amount - discount_amount ELSE 0 END) AS pending_amount
FROM ride_hailing_tripe_project.trips.silver_trips_clean
GROUP BY payment_method;

CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality AS
SELECT COUNT(*)                                                AS bronze_rows,
       COUNT(*) - COUNT(DISTINCT trip_id)                      AS duplicate_rows,
       SUM(CASE WHEN distance_km <= 0 THEN 1 ELSE 0 END)       AS invalid_distance_rows,
       SUM(CASE WHEN rate_per_km < 0 THEN 1 ELSE 0 END)        AS negative_rate_rows,
       SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END)           AS missing_city_rows,
       SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS missing_payment_rows,
       SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END)    AS missing_fare_rows,
       SUM(CASE WHEN customer_rating IS NULL THEN 1 ELSE 0 END) AS missing_rating_rows,
       (SELECT COUNT(*) FROM ride_hailing_tripe_project.trips.silver_trips_clean)    AS clean_rows,
       (SELECT COUNT(*) FROM ride_hailing_tripe_project.trips.silver_trips_rejected) AS rejected_rows
FROM ride_hailing_tripe_project.trips.bronze_ride_trips;