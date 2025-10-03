import pandas as pd
import os

# Generate Simple moving average column
def SMA(df : pd.DataFrame, length=20):
    df[("sma" + str(length))] = pd.to_numeric(df['Close'].rolling(length).mean())
    return df

# Generate Returns column
def pctChange(df : pd.DataFrame):
    df['pctchange'] = pd.to_numeric(df['Close'].pct_change())
    return df

# Generate Exponential Moving Average column
def EMA(df : pd.DataFrame, length=21):
    df[("ema" + str(length))] = pd.to_numeric(df['Close'].ewm(span=length, adjust=False).mean())
    return df

# Generate volatility column
def volatility(df : pd.DataFrame, length=10):
    df[('volatility')] = pd.to_numeric(df['pctchange'].rolling(length).std())
    return df