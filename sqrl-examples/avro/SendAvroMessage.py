from confluent_kafka import Producer
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import io

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9094'
}

# Load Avro schema
schema = avro.schema.parse("""
{
  "type": "record",
  "name": "Order",
  "fields": [
    {
      "name": "id",
      "type": "long"
    },
    {
      "name": "customerid",
      "type": "long"
    },
    {
      "name": "time",
      "type": "string"
    },
    {
      "name": "entries",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderEntry",
          "fields": [
            {
              "name": "productid",
              "type": "int"
            },
            {
              "name": "quantity",
              "type": "int"
            },
            {
              "name": "unit_price",
              "type": "double"
            },
            {
              "name": "discount",
              "type": ["null", "double"],
              "default": null
            }
          ]
        }
      }
    }
  ]
}
""")

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
