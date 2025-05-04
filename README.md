
# 🎵 Spotify Songs Analytics with Snowflake

This project loads and transforms the [900K Spotify dataset](https://www.kaggle.com/datasets/devdope/900k-spotify/data) using **Snowflake SQL**. It demonstrates how to ingest semi-structured JSON data, stage it, create a data warehouse schema, transform it, and prepare it for analysis—all within Snowflake.

---

 🔧 Tools & Technologies

* **Snowflake**: Cloud Data Warehouse
* **SQL**: Data Definition and Manipulation
* **Variant Columns**: For loading JSON
* **Kaggle Dataset**: [900K Spotify Songs](https://www.kaggle.com/datasets/devdope/900k-spotify/data)

---

## 📁 Project Structure

1. **Warehouse Creation**
   Create an XSMALL warehouse with auto suspend/resume:

   ```sql
   CREATE WAREHOUSE Spotify WITH warehouse_size='XSMALL' auto_suspend=60 auto_resume=TRUE;
   ```

2. **Database and Schema Setup**

   ```sql
   CREATE DATABASE Spotify;
   CREATE SCHEMA raw_data;
   CREATE SCHEMA Production;
   ```

3. **Staging and Loading Data**

   ```sql
   CREATE OR REPLACE STAGE spotify_raw_data_stage;
   CREATE TABLE spotify_json_data (raw VARIANT);
   ```

   ✅ JSON file is loaded into this table using the `PUT` command (run through SnowSQL CLI).

4. **Data Transformation**

   Final table created from raw JSON variant column with properly typed fields:

   ```sql
   CREATE TABLE Production.Spotify_master_data AS
   SELECT
     RAW:"artist"::STRING AS artist,
     RAW:"song"::STRING AS song,
     RAW:"emotion"::STRING AS emotion,
     ROUND(RAW:"variance"::FLOAT, 2) AS variance,
     ...
     ROUND(RAW:"Instrumentalness"::FLOAT, 2) AS instrumentalness
   FROM RAW_DATA.SPOTIFY_JSON_DATA;
   ```

5. **Data Cleaning**

   Removing parentheses and unwanted leading spaces from artist names:

   ```sql
   UPDATE Production.SPOTIFY_MASTER_DATA
   SET artist = REPLACE(REPLACE(artist, '(', ''), ')', '')
   WHERE artist LIKE '%(%)%';

   UPDATE Production.SPOTIFY_MASTER_DATA
   SET artist = SUBSTR(artist, 2)
   WHERE artist LIKE ' %';
   ```

6. **Create View for Analytics**

   Creating a view to fetch the most popular song per artist:

   ```sql
   CREATE VIEW Most_popular_song AS
   SELECT *
   FROM (
     SELECT artist, song, popularity,
       ROW_NUMBER() OVER (PARTITION BY artist ORDER BY popularity DESC) AS most_popular_song
     FROM PRODUCTION.SPOTIFY_MASTER_DATA
   )
   WHERE most_popular_song = 1
   ORDER BY artist;
   ```

---

## 🚀 How to Use

1. Clone the repository
2. Set up Snowflake and SnowSQL CLI
3. Run the SQL scripts in the order provided
4. Use the `Most_popular_song` view for analytical queries

---

## 📊 Example Query

```sql
SELECT * FROM Most_popular_song WHERE artist = 'ABBA';
```

---

## 📌 Notes

* JSON data must be compressed using gzip (`.gz`) before uploading to Snowflake Stage
* Use SnowSQL CLI for the `PUT` command
* Snowflake variant columns are powerful for semi-structured data

---

## 🧠 Learning Outcomes

* How to work with JSON in Snowflake
* Creating views and transformations on raw semi-structured data
* Cleaning and optimizing data for analytics

