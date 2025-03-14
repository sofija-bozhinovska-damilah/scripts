import pandas as pd
from kestra import Kestra
import sys

# Read the parquet file into a DataFrame
# Get the input file path from the command line arguments
input_file_path = sys.argv[1]

# Read the parquet file into a DataFrame
df = pd.read_parquet(input_file_path)

# Group by year and find the most sold product for each year
most_sold_products = df.groupby('Year')['Item'].agg(lambda x: x.value_counts().idxmax())

# Save the output to a parquet file
most_sold_products.to_frame(name='MostSoldProduct').to_parquet('MostSoldProductsPerYear.parquet')

# outputs = {
#     'MostSoldProductsPerYear': most_sold_products
# }

# Kestra.outputs(outputs)