from confluent_kafka import Producer
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import io

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9094'
}

# Load Avro schema
schema_path = 'ecommerce-avro/orders.avsc'
schema = avro.schema.parse(open(schema_path, "rb").read())


# Create an Avro record
record = {
    "id": 88046,
    "customerid": 7648,
    "time": "2023-12-07T20:02:07.694714",
    "entries": [
        {"productid": 2586, "quantity": 1, "unit_price": 498.83, "discount": None}
    ]
}

# Function to encode record to binary using Avro
def encode_record(record, schema):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(record, encoder)
    return bytes_writer.getvalue()

# Encode the record
encoded_record = encode_record(record, schema)

# Produce the message
producer = Producer(kafka_config)
producer.produce('orders', encoded_record)
producer.flush()

print('Message sent successfully')
