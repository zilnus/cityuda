# Project: Data Modeling with Redshift

## Introduction

A startup called Sparkify wants to anayze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding whats songs users are listening to.

They'd like a data engineer to create a Redshift database with tables designed to optimize queries on song play analysis. My role is to create a database schema and ETL pipeline for this analysis.

## Project Description

In this project, I'll apply what I've learned on data warehouse with Redshift and build an ETL pipeline using Python. To complete the project, I will need to define fact and dimension tables for a star schema and write ETL pipeline that transfers data from JSON files in S3 into these tables in Redshift using Python and SQL.

## Repository

In addition to the data files, the project workspace include five files:
1. `create_tables.py` drops and creates the tables. We run this file to reset the tables before each time we run the ETL script.
2. `etl.py` read and processes files from `song_data` and `log_data` and loads them into Redshift tables
3. `sql_queries.py` contains all sql queries, and is imported into the last two files above.
4. `dwh.cfg` contains Redshift and S3 configuration
5. `README.md` provides discussion on this project

## Database Schema Design

<img src="images/star_schema.jpg" width="100%">

## ETL Pipeline

Song dataset will be processed by python program `etl.py` and data will be stored in table songs and artists

<img src="images/etl_pipe_song.jpg" width="100%">

Log dataset will be processed by python program `etl.py` and data will be stored in table users, time and songplays

<img src="images/etl_pipe_log.jpg" width="100%">

## How to Run

Open the terminal then run below statement so the schema will be created
`python create_tables.py`

After that, we need to run below statement to run ETL pipeline process
`python etl.py`

## Example of Songplay Analysis

Using query below, we could identify that most paid listeners are Female

```SQL
SELECT gender,COUNT(DISTINCT user_id) As total_listener 
FROM 
   (SELECT songplays.user_id, 
           gender, 
           songplays.level 
           FROM songplays INNER JOIN users ON songplays.user_id = users.user_id)     
    AS Q1 
WHERE level='paid' GROUP BY gender;
```

**Query Result**

<img src="images/query_result.jpg" width="100%">

 