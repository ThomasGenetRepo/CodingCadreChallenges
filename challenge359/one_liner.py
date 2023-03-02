# type: ignore
from datetime import datetime
import pandas as pd

df = pd.read_csv('../data/challenge359/raw_dates.csv', parse_dates=['Date'], date_parser = lambda x: datetime.strptime(x, "%m/%d/%Y"))

df['sol'] = df['Date'].dt.year.astype(str) + 'Q' + df['Date'].dt.quarter.astype(str)

print(df.sample(50).sort_index())
