import pandas as pd

# Read the parquet file into a DataFrame
df = pd.read_parquet('data/output/SoldItemsPerYear.parquet')

# Group by year and find the most sold product for each year
most_sold_products = df.groupby('Year')['Item'].agg(lambda x: x.value_counts().idxmax())

# Save the output to a parquet file
most_sold_products.to_frame(name='MostSoldProduct').to_parquet('data/output/MostSoldProductsPerYear.parquet')