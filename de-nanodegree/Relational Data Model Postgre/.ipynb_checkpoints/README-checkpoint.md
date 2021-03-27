# Project: Data Modeling with Postgres

## Introduction

A startup called Sparkify wants to anayze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding whats songs users are listening to.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis. My role is to create a database schema and ETL pipeline for this analysis.

## Project Description

In this project, I'll apply what I've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, I will need to define fact and dimension tables for a star schema and write ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Repository

In addition to the data files, the project workspace include six files:
1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.ipynb` read and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` read and processes files from `song_data` and `log_data` and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.
6. `README.md` provides discussion on this project

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

|gender|total_listener|
|---|---|
|F|15|
|M|7|
    

