import requests
import pandas as pd
import pyarrow.parquet as pq


response = requests.get("https://restcountries.com/v3.1/all")
data = response.json()

# Convert to DataFrame and save as Parquet
df = pd.json_normalize(data)
df.to_parquet("raw_data.parquet")