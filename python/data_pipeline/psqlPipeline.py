import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()  # loads .env

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)


# create ohlcv table
def createTable():
    cur = conn.cursor()

    #create table if it doesn't exist in local db
    create = f"""
    CREATE TABLE IF NOT EXISTS ohlcv (
        symbol VARCHAR(10),
        ts DATE,
        open NUMERIC(10, 2),
        high NUMERIC(10,2),
        low NUMERIC(10, 2),
        close NUMERIC(10, 2),
        volume NUMERIC(12),
        UNIQUE(symbol, ts)
    );
    """
    
    cur.execute(create)
    conn.commit()
    cur.close()

#cleans up csv before insertion into db
def clean_csv(csv_path : str, symbol: str) -> pd.DataFrame:
    # Pull in csv
    df = pd.read_csv(csv_path)
    df['symbol'] = symbol

    # The methods used to store the Yfinance data to CSV produce two stray rows at the top of the csv. This removes those rows.
    df.drop([0,1],inplace=True)

    # A 'Price' Column imports with yfinance but it should be labeled 'Date'
    df.rename(columns={'Price': 'Date'}, inplace=True)

    # Check for and handle missing values
    if df.isnull().sum().sum() != 0:
        print("Missing Values Detected. Please try reloading the data. ")
        exit(0)

    # Ensure certain columns are numeric
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['Close'] = pd.to_numeric(df['Close'])
    df['High'] = pd.to_numeric(df['High'])
    df['Low'] = pd.to_numeric(df['Low'])
    df['Open'] = pd.to_numeric(df['Open'])
    df['Volume'] = pd.to_numeric(df['Volume'])

    return df

# insert dataframe into postgres database
def insert_into_db(df):
    cur = conn.cursor()

    rows = [
        (row['symbol'], row['Date'], row['Open'], row['High'],
         row['Low'], row['Close'], row['Volume'])
         for _, row in df.iterrows()
    ]

    insert = f"""
    INSERT INTO ohlcv (
        symbol, ts, open, high, low, close, volume
    )
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, ts) DO NOTHING;
    """

    cur.executemany(insert, rows)
    conn.commit()
    cur.close()
    
def run_pipeline():
    createTable()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            symbol = os.path.splitext(file)[0]
            csv_path = os.path.join('data', file)
            df = clean_csv(csv_path, symbol)
            insert_into_db(df)

            os.remove(csv_path)

if __name__ == "__main__":
    run_pipeline()
conn.close()
    