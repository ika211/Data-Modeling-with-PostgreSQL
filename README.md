# Data Modeling with Postgres

This project is a part of Udacity Data Engineering Nano degree. The project focuses 
on the data modelling aspect of the Database for a **Sparkify** company. 

The main _challenges_ are 
1. Creating the Database and tables.
2. Parsing the JSON files and making a few transformations.
3. Inserting the data from these dataframes into Table.

**_Pandas_** library is used in the project for reading JSONs and transformations.

This is similar to an ETL pipeline, although at a smaller scale. 

## Notebooks

### 1. _test.ipynb_

This is a test notebook, that can be used to check if the tables are created, data is inserted
into the tables.

### 2. _etl.ipynb_

This notebook is where we started out writing the ETL functions and code for individual JSON files
which was then translated to **_etl.py_** to scale out to all the files in the **data** directory.

## Scripts

### 1. _sql_queries.py_

All the SQL queries for creating the database, creating and dropping the tables, inserting
data into these tables is present in this script. This script and its variables are 
imported into other files so that code looks clean and organized.

### 2. _create_tables.py_

The python script has functions in it create the database, tables. It initializes the 
Database so that ETL can be executed.

### 3. _etl.py_

This is where the _magic_ happens. The functions and code written in the etl notebook is 
used here except that it will scale out to all the files in the **data** directory
