from flask import request, jsonify
from flasgger import swag_from
from confluent_kafka import (
    Producer, 
    Consumer, 
    KafkaException
)
from . import api 

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092'  # Kafka broker address
}
producer = Producer(conf)


# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'flask-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['test-topic'])


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@api.route(
    '/produce', 
    methods=['POST']
)
@swag_from({
    "tags": ["kafka"],
    "summary": "Produce a message to Kafka",
    "description": "Send a message to a specified Kafka topic.",
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "description": "Message and topic to produce",
            "schema": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "The message to send to the Kafka topic",
                        "example": "test message"
                    },
                    "topic": {
                        "type": "string",
                        "description": "The Kafka topic to which the message will be sent",
                        "example": "test-topic",
                        "default": "test-topic"
                    }
                },
                "required": ["message"]
            }
        }
    ],
    "responses": {
        "200": {
            "description": "predic cost in the future"
        }
    }
})
def produce_message():
    message = request.json.get('message')
    topic = request.json.get('topic', 'test-topic')

    # Produce a message to the specified topic
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()  # Force delivery of messages

    return f"Message produced to topic {topic}."


@api.route('/consume', methods=['GET'])
@swag_from({
    "tags": ["kafka"],
    "summary": "Consume a message of Kafka",
    "description": "Consume a message from Kafka topic.",
    "responses": {
        "200": {
            "description": "consume a message from kafka."
        }
    }
})
def consume_message():
    try:
        msg = consumer.poll(timeout=15.0)
        if msg is None:
            return jsonify({'message': 'No message found'})
        if msg.error():
            raise KafkaException(msg.error())
        return jsonify({'message': msg.value().decode('utf-8')})
    except KafkaException as e:
        return jsonify({'error': str(e)}), 500