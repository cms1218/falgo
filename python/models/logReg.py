import psycopg2
from sklearn.linear_model import LogisticRegression
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

def pullData():
    curr = conn.cursor()

    select = f"""
    SELECT * FROM ohlcv
    WHERE symbol = 'AAPL';
    """

    data = curr.execute(select)
    conn.commit()
    curr.close()

    

