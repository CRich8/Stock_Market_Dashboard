# Stock Market Dashboard

This project contains an ELT pipeline that uses Airflow, DBT and Google Cloud Platform to extract stock market data from [Alpha Vantage](https://www.alphavantage.co/) and transforms it into a single database for analytics applications.

## Overview

Airflow, hosted by Docker, is used to orchestrate the ingestion of stock data into Google Cloud Storage daily. DBT is used to clean and transform the data so that it is ready for analytics. The database was designed with a star schema-like design, where daily stock data are stored in a fact table which relates to other dimensions (e.g., Company overview, Income statement). Finally, Tableau is used to visualize the database as a dashboard.

Please [visit the dashboard here.](https://public.tableau.com/views/StockMArketDashboard/Dashboard1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link)

The technologies used in the pipeline are listed below.

-GCP: BigQuery and Google Cloud Storage were used to store and query the data.

-Terraform: Terraform was used to manage GCP resources.

-Docker: Docker was used to host Airflow.

-DBT: DBT was used to transform and model the data in BigQuery.

-Airflow: Airflow was used to orchestrate the data ingestion to GCP.
