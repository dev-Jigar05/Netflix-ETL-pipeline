import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

# EXTRACT 
def extract():
    df = pd.read_csv("../data/netflix.csv")
    return df

# TRANSFORM 
def transform(df):
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    df['director'] = df['director'].fillna("Unknown")
    df['cast'] = df['cast'].fillna("Unknown")
    df['country'] = df['country'].fillna("Unknown")

    df = df.dropna()

    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df['year_added'] = df['date_added'].dt.year
    df['month_added'] = df['date_added'].dt.month

    df[['duration_number', 'duration_type']] = df['duration'].str.extract(r'(\d+)\s*(\w+)')

    df = df.drop('duration', axis=1)

    df['listed_in'] = df['listed_in'].str.split(',')
    df = df.explode('listed_in')
    df['listed_in'] = df['listed_in'].str.strip()

    # Save output
    os.makedirs("../output", exist_ok=True)
    df.to_csv("../output/netflix_cleaned.csv", index=False)

    return df

# LOAD
def load(df):
    connection_string = os.getenv("DB_CONNECTION_STRING")

    if not connection_string:
        raise ValueError("DB_CONNECTION_STRING not set in .env")

    engine = create_engine(connection_string)

    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Connection Test:", result.fetchone())

    # Load data
    df.to_sql(
        "netflix_cleaned",
        con=engine,
        schema="dbo",
        if_exists="replace",
        index=False
    )

# MAIN 
if __name__ == "__main__":
    print("Running ETL pipeline...")

    df = extract()
    df = transform(df)
    load(df)

    print("Done. Data loaded into SQL Server")