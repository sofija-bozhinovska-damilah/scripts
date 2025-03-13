import pandas

data2019 = pandas.read_csv("data/2019.csv")
data2020 = pandas.read_csv("data/2020.csv")
data2021 = pandas.read_csv("data/2021.csv")

# Create a new dataframe that is a union of the three
data_union = pandas.concat([data2019, data2020, data2021])

# Extract year from "orderDate" and add it as a new column
data_union['Year'] = pandas.to_datetime(data_union['OrderDate']).dt.year

# Group by "Item" and "Year"
grouped_data = data_union.groupby(["Item", "Year"]).size().sort_values(ascending=False)

# Save the output to a parquet file
grouped_data.to_frame(name='Count').reset_index().to_parquet("data/output/SoldItemsPerYear.parquet")

