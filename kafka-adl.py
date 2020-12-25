from kafka import KafkaConsumer
import json
import pandas as pd
from io import StringIO,BytesIO
from datetime import datetime,timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

AZURE_STORAGE_CONNECTION_STRING="Access-Key"

# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

# Azure Container Name
container_name = 'newcontainer'

consumer = KafkaConsumer(
    "topic-name",
    bootstrap_servers='IP-Addr:9092',
    auto_offset_reset='earliest',
    group_id='Consumer Group ID',
    value_deserializer=lambda m:json.loads(m.decode('utf-8')))


print("starting the consumer")
i = 0
l1 = []
for msg in consumer:
    test = msg.value
    l1.append(test)
    i = i + 1
    if i >= 6000:
        df = pd.DataFrame(l1)
        f = BytesIO()
        df.to_parquet(f)
        f.seek(0)
        blob_client = blob_service_client.get_container_client(container=container_name)
        current_date = datetime.now()
        blob_client.upload_blob('customer', data=f, overwrite=True)
        i = 0
        l1 = []
        print('Uploading')


