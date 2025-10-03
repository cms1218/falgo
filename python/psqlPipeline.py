import pandas as pd
import features
import os
import psycopg2

# initialize connection for all functions
conn = psycopg2.connect(
        dbname="falgo",
        user="postgres", 
        password="121805++", 
        host = 'localhost',
        port=5432
    )

# create ohlcv table
def createTable():
    cur = conn.cursor()

    #create table for specific ticker in psql database
    create = f"""
    CREATE TABLE IF NOT EXISTS ohlcv_data (
        symbol VARCHAR(10),
        ts DATE,
        open NUMERIC(10, 2),
        high NUMERIC(10,2),
        low NUMERIC(10, 2),
        close NUMERIC(10, 2),
        volume NUMERIC(12),
        sma20 NUMERIC(10,2),
        sma50 NUMERIC(10,2),
        sma100 NUMERIC(10,2),
        ema9 NUMERIC(10,2),
        ema21 NUMERIC(10,2),
        pctchange NUMERIC(10, 2),
        volatility NUMERIC(10, 2),
        UNIQUE(symbol, ts)
    );
    """
    
    cur.execute(create)
    conn.commit()
    cur.close()

# function to clean up the csv
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

    # Add new columns to the dataframe of popular financial indicators
    df = features.SMA(df, 20)
    df = features.SMA(df, 50)
    df = features.SMA(df, 100)
    df = features.EMA(df, 9)
    df = features.EMA(df, 21)
    df = features.pctChange(df)
    df = features.volatility(df, 10)

    return df

# insert dataframe into postgres database
def insert_into_db(df):
    cur = conn.cursor()

    rows = [
        (row['symbol'], row['Date'], row['Open'], row['High'],
         row['Low'], row['Close'], row['Volume'], row['sma20'],
         row['sma50'], row['sma100'], row['ema9'], row['ema21'],
         row['pctchange'], row['volatility'])
         for _, row in df.iterrows()
    ]

    insert = f"""
    INSERT INTO ohlcv_data (
        symbol, ts, open, high, low, close, volume, 
        sma20, sma50, sma100, ema9, ema21, pctchange, volatility
    )
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    