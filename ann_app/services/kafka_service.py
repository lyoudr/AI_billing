from confluent_kafka import Producer

# Configuration for kafka cluster
conf = {
    "bootstrap.servers": "", # e.g., 'your-kafka-broker:9092'
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "username",
    "sasl.password": "password",
}

# Create a Producer instance
producer = Producer(conf)

# Callback function for successful message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
# Produce a message
def produce_message(topic, message):
    producer.produce(topic, message, callable=delivery_report)


if __name__ == "__main__":
    produce_message()