from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from google.cloud import storage
import json
import asyncio

def send_email_confirmation(id_transaction, customer, timestamp, amount, type_transaction):
    #email_customer = get_email_customer(customer)

    #send_email_to_customer = send_email(email_customer, id_transaction, timestamp, amount, type_transaction)
    send_email_to_customer = 1

    email_confirmation = {
        'idTransaction' : id_transaction,
        'isSendEmail' : send_email_to_customer
    }

    return email_confirmation

async def save_json_in_gcs(kind, data_json, id_transaction, bucket):

    name_file_gcs = bucket.blob(kind + '/' + id_transaction + '_'+ kind+'.json')

    data = json.dumps(data_json)
    name_file_gcs.upload_from_string(data, content_type="application/json")

    return data_json


client = storage.Client.from_service_account_json(json_credentials_path='service-account.json')

bucket = client.get_bucket('df-11-group-1')

schema_registry_conf = {'url': 'http://34.101.60.4:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("schema/TransactionSchema.avsc") as f:
    transaction_schema = f.read()
avro_deserializer = AvroDeserializer(schema_registry_client, transaction_schema)

consumer_conf = {'bootstrap.servers': '34.101.60.4:9092,34.101.60.4:9093,34.101.60.4:9094','group.id': 'non_fraud_transaction'}
consumer = Consumer(consumer_conf)
topic = 'non_fraud_transaction'
consumer.subscribe([topic])

while True:
    msg = consumer.poll(0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # Partisi telah mencapai akhir, lanjutkan ke partisi berikutnya
            continue
        else:
            # Kesalahan lainnya
            print('Error: {}'.format(msg.error().str()))
            print('no')
            continue
    non_fraud_transaction = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

    id_transaction = non_fraud_transaction['idTransaction']
    customer = non_fraud_transaction['nameOrigin']
    timestamp = non_fraud_transaction['timestamp']
    amount = non_fraud_transaction['amount']
    type_transaction = non_fraud_transaction['type']

    save_transaction = asyncio.run(save_json_in_gcs('transaction', non_fraud_transaction, id_transaction,bucket))

    #save_transaction = save_json_in_gcs('transaction', non_fraud_transaction, id_transaction,bucket)
    #save_transaction = save_json_in_gcs('test', non_fraud_transaction, id_transaction,bucket)
    print(save_transaction)

    is_send_email = send_email_confirmation(id_transaction, customer, timestamp, amount, type_transaction)
    
    save_email_confirmation = asyncio.run(save_json_in_gcs('email_confirmation', is_send_email, id_transaction,bucket))
    #save_email_confirmation = save_json_in_gcs('email_confirmation', is_send_email, id_transaction,bucket)
    #save_email_confirmation = save_json_in_gcs('test', is_send_email, id_transaction,bucket)
    print(save_email_confirmation)