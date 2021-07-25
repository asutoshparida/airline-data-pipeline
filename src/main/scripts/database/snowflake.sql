create or replace warehouse INTERVIEW_WH warehouse_size=X-Small initially_suspended=true;
create transient database USER_ASUTOSH;

USE WAREHOUSE INTERVIEW_WH;
USE USER_ASUTOSH;

create schema "USER_ASUTOSH"."CURATED";
GRANT SELECT ON ALL TABLES IN SCHEMA "CURATED" TO ROLE PUBLIC;

-- Craete Table AIRLINE_AGGREGATED
--CREATE TABLE "USER_ASUTOSH"."CURATED"."AIRLINE_AGGREGATED" ("YEAR" INTEGER, "MONTH" INTEGER, "DAY" INTEGER, "DAY_OF_WEEK" INTEGER, "FLIGHT_NUMBER" INTEGER, "TAIL_NUMBER" STRING, "ORIGIN_AIRPORT" STRING, "DESTINATION_AIRPORT" STRING, "SCHEDULED_DEPARTURE" INTEGER, "DEPARTURE_TIME" INTEGER, "DEPARTURE_DELAY" INTEGER, "TAXI_OUT" INTEGER, "WHEELS_OFF" INTEGER, "SCHEDULED_TIME" INTEGER, "ELAPSED_TIME" INTEGER, "AIR_TIME" INTEGER, "DISTANCE" INTEGER, "WHEELS_ON" INTEGER, "TAXI_IN" INTEGER, "SCHEDULED_ARRIVAL" INTEGER, "ARRIVAL_TIME" INTEGER, "ARRIVAL_DELAY" INTEGER, "DIVERTED" INTEGER, "CANCELLED" INTEGER, "CANCELLATION_REASON" STRING, "AIR_SYSTEM_DELAY" INTEGER, "SECURITY_DELAY" INTEGER, "AIRLINE_DELAY" INTEGER, "LATE_AIRCRAFT_DELAY" INTEGER, "WEATHER_DELAY" INTEGER, "EVENT_DATE" DATE, "AIRLINE_NAME" STRING, "TOTAL_DELAY" INTEGER, "ORIGIN_AIRPORT_NAME" STRING, "DESTINATION_AIRPORT_NAME" STRING, "ORIGIN_AIRPORT_CITY" STRING, "DESTINATION_AIRPORT_CITY" STRING, "ORIGIN_AIRPORT_STATE" STRING, "DESTINATION_AIRPORT_STATE" STRING, "ORIGIN_AIRPORT_COUNTRY" STRING, "DESTINATION_AIRPORT_COUNTRY" STRING, "ORIGIN_AIRPORT_LATITUDE" STRING, "DESTINATION_AIRPORT_LATITUDE" STRING, "ORIGIN_AIRPORT_LONGITUDE" STRING, "DESTINATION_AIRPORT_LONGITUDE" STRING);

select ARRIVAL_DELAY,DEPARTURE_DELAY, TOTAL_DELAY from CURATED.AIRLINE_AGGREGATED;
select * from CURATED.AIRLINE_AGGREGATED;

--Task Wise Views

CREATE VIEW CURATED.task1_view (flight_count, year, month, airline_name, origin_airport_name) AS
select count(1) as flight_count, year, month, airline_name, origin_airport_name from CURATED.AIRLINE_AGGREGATED group by 2,3,4,5;

CREATE VIEW CURATED.task2_view (ontime_percnt, airline_name) AS
with
total_flights as (select count(1) as total_count, airline_name from CURATED.AIRLINE_AGGREGATED group by 2),
on_time_flights as (select count(1) as ontime_count, airline_name from CURATED.AIRLINE_AGGREGATED ag where arrival_delay <= 0 group by 2)
select (otf.ontime_count/tf.total_count) as ontime_percnt, tf.airline_name from total_flights as tf INNER JOIN on_time_flights as otf ON tf.airline_name = otf.airline_name;

CREATE VIEW CURATED.task6_view (total_unique_routes, airline_name) AS
with
most_unique_routes as (select count(1) as no_of_flights, ORIGIN_AIRPORT, DESTINATION_AIRPORT from CURATED.AIRLINE_AGGREGATED group by 2,3 having count(1) = 1),
most_unique_airlines as (select mur.*, ag.AIRLINE_NAME from most_unique_routes mur INNER JOIN curated.airline_aggregated ag on ag.ORIGIN_AIRPORT = mur.ORIGIN_AIRPORT AND ag.DESTINATION_AIRPORT = mur.DESTINATION_AIRPORT)
select sum(mua.no_of_flights) as total_unique_routes, AIRLINE_NAME from most_unique_airlines mua group by 2;


CREATE VIEW CURATED.task4_view (total_cancelled, CANCELLATION_REASON, ORIGIN_AIRPORT_NAME) AS
select count(1) as total_cancelled,
case when CANCELLATION_REASON = 'A' then 'Airline/Carrier'
when CANCELLATION_REASON = 'B' then 'Weather'
when CANCELLATION_REASON = 'C' then 'National Air System'
when CANCELLATION_REASON = 'D' then 'Security' END as CANCELLATION_REASON,
 ORIGIN_AIRPORT_NAME from CURATED.AIRLINE_AGGREGATED where CANCELLATION_REASON is not null group by 2,3;

CREATE VIEW CURATED.task3_view (delayed_flights_count, airline_name) AS
select count(1) as delayed_flights_count,
 airline_name from CURATED.AIRLINE_AGGREGATED where TOTAL_DELAY > 0 group by 2 order by 1 DESC;


//Task1
select count(1) as flight_count, year, month, airline_name, origin_airport_name from CURATED.AIRLINE_AGGREGATED group by 2,3,4,5;
//Task2
with
total_flights as (select count(1) as total_count, airline_name from CURATED.AIRLINE_AGGREGATED group by 2),
on_time_flights as (select count(1) as ontime_count, airline_name from CURATED.AIRLINE_AGGREGATED ag where arrival_delay <= 0 group by 2)
select (otf.ontime_count/tf.total_count) as ontime_percnt, tf.airline_name from total_flights as tf INNER JOIN on_time_flights as otf ON tf.airline_name = otf.airline_name;
//Task3
select count(1) as delayed_flights,
 airline_name from CURATED.AIRLINE_AGGREGATED where TOTAL_DELAY > 0 group by 2 order by 1 DESC;
//Task4
select count(1) as total_cancelled,
case when CANCELLATION_REASON = 'A' then 'Airline/Carrier'
when CANCELLATION_REASON = 'B' then 'Weather'
when CANCELLATION_REASON = 'C' then 'National Air System'
when CANCELLATION_REASON = 'D' then 'Security' END as CANCELLATION_REASON,
 ORIGIN_AIRPORT_NAME from CURATED.AIRLINE_AGGREGATED where CANCELLATION_REASON is not null group by 2,3;
//Task6
with
most_unique_routes as (select count(1) as no_of_flights, ORIGIN_AIRPORT, DESTINATION_AIRPORT from CURATED.AIRLINE_AGGREGATED group by 2,3 having count(1) = 1),
most_unique_airlines as (select mur.*, ag.AIRLINE_NAME from most_unique_routes mur INNER JOIN curated.airline_aggregated ag on ag.ORIGIN_AIRPORT = mur.ORIGIN_AIRPORT AND ag.DESTINATION_AIRPORT = mur.DESTINATION_AIRPORT)
select sum(mua.no_of_flights), AIRLINE_NAME from most_unique_airlines mua group by 2;