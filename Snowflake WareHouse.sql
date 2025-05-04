-- Creating WareHouse
create warehouse Spotify with warehouse_size='XSMALL' auto_suspend=60 auto_resume=True;

--Checking if Warehouse is create or not 
SHOW WAREHOUSES

--creating database
create database Spotify;

--creating Schema for raw data
create schema raw_data;

--using the raw data schema 
Use Schema raw_data;

--create stage for file 
create or replace stage spotify_raw_data_stage;


--creating table to get raw data 
USE DATABASE Spotify;
USE SCHEMA raw_data;

CREATE TABLE spotify_json_data (
    raw VARIANT
);

-- data stage to table transfer

COPY INTO spotify_json_data FROM @spotify_raw_data_stage FILE_FORMAT = (TYPE = 'JSON');

--checking if data is loaded correctly
select * from SPOTIFY_JSON_DATA limit 5

-- creating new schema for final data data 
create schema Production;
use schema Production;


--creating final data 
create table
Spotify_master_data as 
SELECT
  RAW:"artist"::STRING AS artist,
  RAW:"song"::STRING AS song,
  RAW:"emotion"::STRING AS emotion,
  ROUND(RAW:"variance"::FLOAT, 2) AS variance,
  RAW:"Genre"::STRING AS genre,
  RAW:"Release Date"::INT AS release_date,
  RAW:"Key"::STRING AS key_signature,
  ROUND(RAW:"Tempo"::FLOAT, 2) AS tempo,
  ROUND(RAW:"Loudness"::FLOAT, 2) AS loudness,
  RAW:"Explicit"::STRING AS explicit,
  ROUND(RAW:"Popularity"::FLOAT, 2) AS popularity,
  ROUND(RAW:"Energy"::FLOAT, 2) AS energy,
  ROUND(RAW:"Danceability"::FLOAT, 2) AS danceability,
  ROUND(RAW:"Positiveness"::FLOAT, 2) AS positiveness,
  ROUND(RAW:"Speechiness"::FLOAT, 2) AS speechiness,
  ROUND(RAW:"Liveness"::FLOAT, 2) AS liveness,
  ROUND(RAW:"Acousticness"::FLOAT, 2) AS acousticness,
  ROUND(RAW:"Instrumentalness"::FLOAT, 2) AS instrumentalness
FROM
  RAW_DATA.SPOTIFY_JSON_DATA;

-- remove few adultration from Artist name column

update Production.SPOTIFY_MASTER_DATA
set 
artist=replace(replace(artist,'(',''),')','')
where 
artist like '%(%%)%';


update Production.SPOTIFY_MASTER_DATA
set 
artist=substr(artist,2)
where 
artist like ' %';

commit;

use database SPOTIFY;


use schema Production;
create view Most_popular_song as
SELECT * 
FROM (
    SELECT artist, song, popularity,
           ROW_NUMBER() OVER (PARTITION BY artist ORDER BY popularity DESC) AS most_popular_song
    FROM PRODUCTION.SPOTIFY_MASTER_DATA
)
WHERE most_popular_song = 1
ORDER BY artist;
