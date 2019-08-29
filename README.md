<p align="center">
  <img width="530" height="270" src="https://upload.cc/i1/2019/08/26/pAfdKM.jpg">
</p>

# Cloud Data Warehouse (By using AWS Redshift)
#### PROJECT BACKGROUND AND SUMMARY
###### *BACKGROUND*
Sparkify is a startup company which provides the music streaming app. Recently, the analytics team in this company is interested in understanding their user activity on its music streaming app in order to provide better user experience for their user. Their user activities data resides in AWS S3, which are in JSON format. This project aims for **building a data pipeline for copying user activities data from AWS S3 to staging tables** and **for moving the data from staging tables to final analytical tables**. By having the analytical tables resided in Redshift, the analytics team can analyze the data, give suggestions to the APP development team, and improve the product. 

###### *DETAILS AND DATA MODELING*
In this project, it will create an ETL data pipeline to extract JSON data from AWS S3, copy data from AWS S3 to staging tables in Redshift, transform data into a format which analytics team prefers, and move data from staging tables to final analytical tables. For the final analytical table, this project decides to use star schema to store the data and improve access to data. 
The fact table is songplay, it includes information about songplay history. The dimension tables are user, song, artist, and time. User table includes the user's personal information. Song table includes the song's information. Time table includes when a song is played. The structure can be seen in the below picture.

<p align="center">
  <img width="530" height="270" src="https://upload.cc/i1/2019/08/25/gM9qd6.jpg">
</p>

------------
#### FILES IN THE REPOSITORY
1. **sql_queries.py**: a python script which details all SQL queries are used in **create_tables.py** and **etl.py**. 

2. **create_tables.py**: a python script which is used for dropping tables (if the tables exist), creating schemas, and creating staging tables and final analytical table.

3. **etl.py**: a python script which is used for copying data from S3 to Redshift staging tables, transforming data into a format this project want, and moving data from staging tables to final analytical tables.

4. **dwh.cfg**: a configuration file which contains the necessary information of connecting to S3 and Redshift

5. **code_develop_notebook.ipynb**: a jupyter notebook file which is written for developing the python code for create_tables.py and etl.py. If you want to do the further modification, you can run the command in this file first.

------------
#### HOW TO RUN THE PROJECT
To start the project, you need **create_tables.py** and **etl.py**. Steps are listed below.
1. access to your AWS account, create an IAM for Redshift cluster, and launch a Redshift Cluster

2. modify **dwf.cfg** file. You need modify the content in [CLUSTER] and [IAM_ROLE] section. (Based on your IAM information and Redshift cluster information)

2. type `python3 create_tables.py` in your terminal to create schema and table in Redshift

3. type `python3 etl.py` in your terminal to start the process of copying data from S3 to Redshift staging tables, transforming data into the format which fits the star schema, and moving data from Redshift staging tables to Redshift analytical tables. 



