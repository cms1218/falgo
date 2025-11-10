import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd

#connect to psql db
load_dotenv()
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")   
)

#pull ticker information from db
def pullData(ticker : str):
    curr = conn.cursor()

    select = """
    SELECT * FROM ohlcv
    WHERE symbol = %s;
    """

    curr.execute(select, (ticker,))
    rows = curr.fetchall()
    cols = [desc[0] for desc in curr.description]
    curr.close()

    return pd.DataFrame(rows=rows, cols=cols)
