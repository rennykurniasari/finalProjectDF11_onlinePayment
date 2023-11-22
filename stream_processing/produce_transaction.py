from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Producer
from datetime import datetime, timedelta
import csv
import uuid
from io import StringIO

from google.cloud import storage

schema_registry_conf = {'url': 'http://34.101.60.4:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("schema/TransactionSchema.avsc") as f:
    transaction_schema = f.read()
avro_serializer = AvroSerializer(schema_registry_client, transaction_schema)

producer_conf = {'bootstrap.servers': '34.101.60.4:9092,34.101.60.4:9093,34.101.60.4:9094'}
producer = Producer(producer_conf)

start_date = datetime(2023, 10, 1, 0, 0, 0)

client = storage.Client.from_service_account_json(json_credentials_path='service-account.json')

bucket = client.get_bucket('df-11-group-1')
print("passed 0")
blob = bucket.blob('online_payment_log_Oct2023.csv')
print("passed 1")
blob = blob.download_as_string()
print("passed 2")
blob = blob.decode('utf-8')
print("passed 3")
blob = StringIO(blob)  #tranform bytes to string here
print("passed 4")

csv_reader = csv.reader(blob)
print("passed 5")
next(csv_reader)
print("passed 6")
'''
with open('Fraud Transaction Dataset/online_payment_log_Oct2023.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    # Melewati baris header pertama
    next(csv_reader)
'''
for row in csv_reader:
        is_fraud = int(row[10])

        transaction = {
            'idTransaction' : row[0],
            'timestamp' : row[1],
            'type' : row[2],
            'amount': float(row[3]),
            'nameOrigin': row[4],
            'oldBalanceOrigin': float(row[5]),
            'newBalanceOrigin': float(row[6]),
            'nameDest': row[7],
            'oldBalanceDest': float(row[8]),
            'newBalanceDest': float(row[9]),
            'isFraud': is_fraud,
            'isFlaggedFraud' : int(row[11])
        }

        if is_fraud :
            producer.produce(topic='fraud_transaction',
                    value=avro_serializer(transaction, SerializationContext('fraud_transaction', MessageField.VALUE)))
        else :
            producer.produce(topic='non_fraud_transaction',
                    value=avro_serializer(transaction, SerializationContext('non_fraud_transaction', MessageField.VALUE)))
        print(transaction)

        producer.flush()
        break