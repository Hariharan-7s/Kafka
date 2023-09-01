from influxdb_client import InfluxDBClient, WriteOptions
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def dataframe_to_influxdb(df, client):
    data = df
    print(data)

    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []
    # print(df)
    # Create a new column 'tag_key=tag_value' with formatted values

    for _, row in data.iterrows():
        point = Point(row['measurement']).time(
            row['timestamp'], WritePrecision.NS)

        # Process fields
        for field in row['field_key=field_value'].split(','):
            key, value = field.split('=')
            point = point.field(key, float(value))

        points.append(point)

    # Write the data to InfluxDB
    write_api.write(bucket="sample", org="Hari", record=points)
    print("inserted successfully ")
    '''




# InfluxDB connection details
influxdb_url = "http://localhost:8086"  # Replace with your InfluxDB URL
# Replace with your InfluxDB token
influxdb_token = "QQD7l6jdFELVdtTf2BkZ6v_5aZD6G3IWjqo6X-BHjqtnEYP7ZHEHcUqUkoGq598j99qexgQQ_BHjcKxKnFHIqQ=="
influxdb_org = "Hari"  # Replace with your InfluxDB organization
influxdb_bucket = "sample"  # Replace with your InfluxDB bucket

time_interval = "1h"
# Create an InfluxDB client
client = InfluxDBClient(
    url=influxdb_url, token=influxdb_token, org=influxdb_org)

dataframe_to_influxdb(client)
'''
# Sample data (replace with your DataFrame)


"""
df = pd.read_csv("output.csv", usecols=['timestamp', 'cup_usage', 'COMMAND'])
print(df)
# Convert 'timestamp' to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d-%m-%Y %H:%M')
# max
grouped = df.groupby('timestamp').apply(
    lambda x: x.loc[x['cup_usage'].idxmax()])

processed_data = grouped[['COMMAND', 'cup_usage']].copy()
print('max: ', processed_data)


# average
agg_funcs = {'cup_usage': ['mean']}
result = df.groupby('timestamp').agg(agg_funcs)
print("avg: ", result)

#df2 = df[['COMMAND', 'cup_usage', 'timestamp']].copy()
# print(df2)
# print(result)
"""

'''


def dffun(df):
    selected_columns = ['command', 'cpu_usage', 'timestamp']
    new_df = df[selected_columns]

    print(new_df)
'''
