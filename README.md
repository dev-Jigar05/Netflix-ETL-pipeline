# Netflix ETL Pipeline

## Description
Built an ETL pipeline to process Netflix dataset using Python and SQL Server.

## Tech Stack
- Python (Pandas)
- SQL Server
- SQLAlchemy
- PyODBC

## Workflow
1. Extracted raw CSV data
2. Cleaned and transformed data (handled missing values, date parsing, duration split, genre processing)
3. Loaded cleaned data into SQL Server
4. Generated output CSV for analysis

## How to Run
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure Environment:
   Create a `.env` file in the root directory and add your SQL Server connection string:
   ```env
   DB_CONNECTION_STRING=mssql+pyodbc://@<SERVER_NAME>\<INSTANCE_NAME>/<DATABASE_NAME>?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes
   ```
   *(Note: Use a single backslash (`\`) for the instance name.)*

3. Run pipeline:
   ```bash
   python scripts/etl_pipeline.py
   ```