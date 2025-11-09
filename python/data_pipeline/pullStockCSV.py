import yfinance as yf
import pandas as pd
import os

#---CONFIG CSV DOWNLOAD---#
TICKER = ('AAPL').strip()
START_DATE = '2024-01-01'
END_DATE = '2025-01-01'
TIME_FRAME = '1d'
SAVEAS = TICKER + '.csv'
FOLDER = 'data'

# Download Stock Data
data = yf.download(TICKER, start = START_DATE, end = END_DATE, interval=TIME_FRAME)

# Save to csv
path = os.path.join(FOLDER, SAVEAS)
data.to_csv(path)
