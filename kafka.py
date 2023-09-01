from confluent_kafka import Consumer, KafkaError
import time
import pandas as pd
from influxdb_client import InfluxDBClient, WriteOptions
import json
from kafkafun import dataframe_to_influxdb


# InfluxDB connection details
influxdb_url = "http://localhost:8086"  # Replace with your InfluxDB URL
# Replace with your InfluxDB token
influxdb_token = "QQD7l6jdFELVdtTf2BkZ6v_5aZD6G3IWjqo6X-BHjqtnEYP7ZHEHcUqUkoGq598j99qexgQQ_BHjcKxKnFHIqQ=="
influxdb_org = "Hari"  # Replace with your InfluxDB organization
influxdb_bucket = "sample"  # Replace with your InfluxDB bucket

# Create an InfluxDB client
client = InfluxDBClient(
    url=influxdb_url, token=influxdb_token, org=influxdb_org)


conf = {
    'bootstrap.servers': '192.168.0.126:9092',
    'group.id': 'anomaly1.0',
    'auto.offset.reset': 'latest',
}
print(conf)
consumer = Consumer(conf)
consumer.subscribe(['dev3.0'])

data_list = []

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            # print("Received message:", message_value)
            json_data = json.loads(message_value)
            data_list.append(json_data)
            df = pd.DataFrame(data_list)

            selected_columns = ['command', 'cpu_usage', 'timestamp']
            df = df[selected_columns]

            # print(df)
            # Create a DataFrame
            df = pd.DataFrame(data_list)

            # Convert 'cpu_usage' to numeric, replacing invalid values with NaN
            df['cpu_usage'] = pd.to_numeric(df['cpu_usage'], errors='coerce')

            # Remove rows with NaN values in 'cpu_usage'
            df = df.dropna(subset=['cpu_usage'])

            # Convert 'timestamp' to datetime
            df['timestamp'] = pd.to_datetime(
                df['timestamp'], format='%Y-%m-%d %H:%M:%S')

            # Group by 'timestamp' and calculate the mean 'cpu_usage'
            df = df.groupby(
                'timestamp')['cpu_usage'].mean().reset_index()
            df['measurement'] = 'Newly_updated_df'
            # Display the result
            # print(df)

            new_rows = []
            for _, row in df.iterrows():
                timestamp = row['timestamp']
                measurement = row['measurement']

                # Replace with actual field information
                fields = f"cpu_usage={row['cpu_usage']}"

                new_row = f"{timestamp},{measurement},{fields}"
                new_rows.append(new_row)

            data = []
            for row in new_rows:
                parts = row.split(',')
                timestamp = pd.to_datetime(parts[0])
                measurement = parts[1]

                field_key, field_value = parts[2].split('=')

                data.append({
                    'timestamp': timestamp,
                    'measurement': measurement,
                    'field_key=field_value': f"cpu={field_value}"
                })

            df = pd.DataFrame(data)

            print(df)

        dataframe_to_influxdb(df, client)
        # df.to_csv('output.csv', index=False)
        time.sleep(1)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

# Convert the JSON data to a DataFrame
