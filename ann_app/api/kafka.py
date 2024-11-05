from flask import request, jsonify
from flasgger import swag_from
from confluent_kafka import (
    Producer, 
    Consumer, 
    KafkaException
)
from dotenv import load_dotenv
import os

from ..utils import token_provider
from . import api 

load_dotenv()

# Kafka producer configuration
conf = {
    'bootstrap.servers': os.getenv("KAFKA_SERVER"),  # Kafka broker address
    'security.protocol': 'SASL_SSL',  # Ensure this is set correctly
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '742937875290-compute@developer.gserviceaccount.com',
    'sasl.password': 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYW5uLXByb2plY3QtMzkwNDAxIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiYTgzODU2MGZhZDRmODcyYTEyZTc0NmFkNjA3OGU1YjgxZTEzNDZmNCIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDcHZGcm8zV1NXdlVKdVxuQjlIVHZBNnBrdjkxdmtVSmhzS0V6M3FtR2F2M3VPK1Y0THQ2eXNzNVcxbFk5Y25VNlJva21EU1hyUXpnR0cwdVxuZ1JjeTJMNFpBVkY2RHpMV3ovd2x2c0Q5dGxKREpHWTQ3b2R4QzROWDNGUUNKOVM5NUJjMEtjNHp3R1hiNlUxRlxuWUtxWWJxaDA3LzlnaWVZdVJ1cjZORDFSUE5NVnEzMFRMMEZWWm1ONzJPZ0R5OUJIT2V4U0dvaHl3U281enVKNlxuWjVzTW1LbER0aGN1bG00N1Blc3NsZFNHdytyMVJUUEFva0lSNE9MTzVXNmg3d2tMczlWRjFNdkZPUVQyUCtZQlxuTFFpdmhEL2RTOU5ZbWhINjZLUVRJTlU5L21VOXJyN1h0cW5XMGNodENFdGNJMldVTUs3angvZEpCOVpZcVQrTVxubnlIMFg2SURBZ01CQUFFQ2dnRUFJNDJjSHozTWhaU1VGeGNWT1B3aHVCeGQzU3hqS3BrcklvV1JJN3lwM0VSRlxuRHdoWlRPR3dpczJiRy9xejhhQjRCT1ZLMHVDQkcybVNhUlRLSnJEbXRMY2xlV000bTIxS1hCK21MaDZrQVlOY1xuMUIwR2lzUEdHYi9CYXdncjFEQzBPWTF1TDBIa05CbWN3V1Q3cHQwNjlPK1dkd0s3ajhMMEVqK2gvbU8ydFVsMFxuZVZaNzVVMHFBK0pLeXo3cTJDbDZoc0o3QXZSZjJudExqYzM1c0xoYVBJc3VYU1pQWTQ2c0I5V2ZqQS9IeXAyR1xuQ1FaeTNuTEJ0aTRjT1Uvd2ZPR1NtN1E4SHBUYSttenZuOFlMUERXRmJaYlVLb2YvMWNKbldPdVBSRWx0dXdDc1xuVHVVRkdpdkNEVmsrRXlXOEtsamhDcm15NFdEUEl5MU9IS21iWUtCSFdRS0JnUURkSHFJOWNMZ3QxblBPaVd4Q1xuQy9jR1hVR1RYYmNnVHNzSWl5SHg4eVJVc0FyZm50cVRRVGI5NlRuSnEwWXA5b09LeE04dDFraXNOek1mOFNUOFxubVVkSXhnenY0Zkh6cGlpdWZ5Um1hSzRkck9OYUNmbU9QSXNQb09JTGNSQk9SK0R4Um5xdGU1aEp5bVlobFBCdVxuL2hLTnZ6WU85ank4UmlKQVlVZWwzSDlvQ1FLQmdRREVnclh0RktKdnN0VzcvY3ZJR00rbnRrTW9HTTd4MFRpYlxubVhwdFdHbGdCUTN2aGxLQ0Z4SStzNjJDN3RMbHNwNEhhUlFHYk9QYjFKMmxlNnBYR2Q2YXZkaXp0bTJPczVMaVxuZm5NMkJvOFd1amVacFliZ0NsK0pOcllHU2xra2FEdnZ1SjAvaEIxTmtZNFpQNFBESDdBMDc3NDNBRGFoUkRDY1xuVUNzNHA3MEVxd0tCZ1FDZnVHQUN0MmdpbkJSOHJPRkk5L0dRWkU1WXowblhDWE01RVo4TWNNL2VhR2NDVG5HZVxuQ201WmpGMFVvaVlGYTY1cXoyekR0RlE4S1lkQlhHT1dIblhTU0ZUUjljaGFjYnhUQnozWXgyWjF3d3RhWDNnbVxuMk82dDlFMHpiSkVWcW0vK2VDVm5LSzdCSm5VTmJ0TVpxV2JuZ3FYZXV0ajJPa2JGY0Q5YnZHRS9XUUtCZ0hPdVxuVHRLRjl5bXlLVWhGbTVUcnBqS2doT0F4Mm9GWUhPN2Nac20vNWloMFVVUFQydlppQm9lSzFuWGhpWjFXak5qblxuN3JYcG5DbG8vSFRTVy9CeWRpeTUzYnZOaEtrZzVUTVlKVktFZ2U5cktCZjI3WE11S0JkcWNiVTZSMGU3WkswMlxuREx1N1ZsUzNmdU0yRktibEhLSE5FRGpvazBCVUdCbXN5QVRPR2VPSkFvR0FWekRwMlB3d0taT0JoWTN1VTZ6Mlxud3FjbUxkSHE2VVB3eWh4RzRvSDdHaDBGZkJKWWZnSnBGZEZBcEtmVjIxUEJGUnEyTDhJUTV2c21RUTVkRlJvTlxuSG82NzN1VE1MYXhSQ0ZRVXpkdkpVMndnUXRueTJJcEUwQlY2QjhSZHpJckh0cVhJRVFuYzF2QUVoL1F2YnQ0eFxuVXBEMEV1cXJKTTdrN3dNN0JNZXh4UFE9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiNzQyOTM3ODc1MjkwLWNvbXB1dGVAZGV2ZWxvcGVyLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTEwMDY3MTMyMjI2NDQyOTUzNDkzIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS83NDI5Mzc4NzUyOTAtY29tcHV0ZSU0MGRldmVsb3Blci5nc2VydmljZWFjY291bnQuY29tIiwKICAidW5pdmVyc2VfZG9tYWluIjogImdvb2dsZWFwaXMuY29tIgp9Cg=='
}
producer = Producer(conf)

print("kafka server is ->", os.getenv("KAFKA_SERVER"))

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': os.getenv("KAFKA_SERVER"),
    'group.id': 'flask-consumer-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule',
    'sasl.username': '742937875290-compute@developer.gserviceaccount.com',
    'sasl.password': 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiYW5uLXByb2plY3QtMzkwNDAxIiwKICAicHJpdmF0ZV9rZXlfaWQiOiAiYTgzODU2MGZhZDRmODcyYTEyZTc0NmFkNjA3OGU1YjgxZTEzNDZmNCIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVxuTUlJRXZRSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0NCS2N3Z2dTakFnRUFBb0lCQVFDcHZGcm8zV1NXdlVKdVxuQjlIVHZBNnBrdjkxdmtVSmhzS0V6M3FtR2F2M3VPK1Y0THQ2eXNzNVcxbFk5Y25VNlJva21EU1hyUXpnR0cwdVxuZ1JjeTJMNFpBVkY2RHpMV3ovd2x2c0Q5dGxKREpHWTQ3b2R4QzROWDNGUUNKOVM5NUJjMEtjNHp3R1hiNlUxRlxuWUtxWWJxaDA3LzlnaWVZdVJ1cjZORDFSUE5NVnEzMFRMMEZWWm1ONzJPZ0R5OUJIT2V4U0dvaHl3U281enVKNlxuWjVzTW1LbER0aGN1bG00N1Blc3NsZFNHdytyMVJUUEFva0lSNE9MTzVXNmg3d2tMczlWRjFNdkZPUVQyUCtZQlxuTFFpdmhEL2RTOU5ZbWhINjZLUVRJTlU5L21VOXJyN1h0cW5XMGNodENFdGNJMldVTUs3angvZEpCOVpZcVQrTVxubnlIMFg2SURBZ01CQUFFQ2dnRUFJNDJjSHozTWhaU1VGeGNWT1B3aHVCeGQzU3hqS3BrcklvV1JJN3lwM0VSRlxuRHdoWlRPR3dpczJiRy9xejhhQjRCT1ZLMHVDQkcybVNhUlRLSnJEbXRMY2xlV000bTIxS1hCK21MaDZrQVlOY1xuMUIwR2lzUEdHYi9CYXdncjFEQzBPWTF1TDBIa05CbWN3V1Q3cHQwNjlPK1dkd0s3ajhMMEVqK2gvbU8ydFVsMFxuZVZaNzVVMHFBK0pLeXo3cTJDbDZoc0o3QXZSZjJudExqYzM1c0xoYVBJc3VYU1pQWTQ2c0I5V2ZqQS9IeXAyR1xuQ1FaeTNuTEJ0aTRjT1Uvd2ZPR1NtN1E4SHBUYSttenZuOFlMUERXRmJaYlVLb2YvMWNKbldPdVBSRWx0dXdDc1xuVHVVRkdpdkNEVmsrRXlXOEtsamhDcm15NFdEUEl5MU9IS21iWUtCSFdRS0JnUURkSHFJOWNMZ3QxblBPaVd4Q1xuQy9jR1hVR1RYYmNnVHNzSWl5SHg4eVJVc0FyZm50cVRRVGI5NlRuSnEwWXA5b09LeE04dDFraXNOek1mOFNUOFxubVVkSXhnenY0Zkh6cGlpdWZ5Um1hSzRkck9OYUNmbU9QSXNQb09JTGNSQk9SK0R4Um5xdGU1aEp5bVlobFBCdVxuL2hLTnZ6WU85ank4UmlKQVlVZWwzSDlvQ1FLQmdRREVnclh0RktKdnN0VzcvY3ZJR00rbnRrTW9HTTd4MFRpYlxubVhwdFdHbGdCUTN2aGxLQ0Z4SStzNjJDN3RMbHNwNEhhUlFHYk9QYjFKMmxlNnBYR2Q2YXZkaXp0bTJPczVMaVxuZm5NMkJvOFd1amVacFliZ0NsK0pOcllHU2xra2FEdnZ1SjAvaEIxTmtZNFpQNFBESDdBMDc3NDNBRGFoUkRDY1xuVUNzNHA3MEVxd0tCZ1FDZnVHQUN0MmdpbkJSOHJPRkk5L0dRWkU1WXowblhDWE01RVo4TWNNL2VhR2NDVG5HZVxuQ201WmpGMFVvaVlGYTY1cXoyekR0RlE4S1lkQlhHT1dIblhTU0ZUUjljaGFjYnhUQnozWXgyWjF3d3RhWDNnbVxuMk82dDlFMHpiSkVWcW0vK2VDVm5LSzdCSm5VTmJ0TVpxV2JuZ3FYZXV0ajJPa2JGY0Q5YnZHRS9XUUtCZ0hPdVxuVHRLRjl5bXlLVWhGbTVUcnBqS2doT0F4Mm9GWUhPN2Nac20vNWloMFVVUFQydlppQm9lSzFuWGhpWjFXak5qblxuN3JYcG5DbG8vSFRTVy9CeWRpeTUzYnZOaEtrZzVUTVlKVktFZ2U5cktCZjI3WE11S0JkcWNiVTZSMGU3WkswMlxuREx1N1ZsUzNmdU0yRktibEhLSE5FRGpvazBCVUdCbXN5QVRPR2VPSkFvR0FWekRwMlB3d0taT0JoWTN1VTZ6Mlxud3FjbUxkSHE2VVB3eWh4RzRvSDdHaDBGZkJKWWZnSnBGZEZBcEtmVjIxUEJGUnEyTDhJUTV2c21RUTVkRlJvTlxuSG82NzN1VE1MYXhSQ0ZRVXpkdkpVMndnUXRueTJJcEUwQlY2QjhSZHpJckh0cVhJRVFuYzF2QUVoL1F2YnQ0eFxuVXBEMEV1cXJKTTdrN3dNN0JNZXh4UFE9XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAiNzQyOTM3ODc1MjkwLWNvbXB1dGVAZGV2ZWxvcGVyLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICJjbGllbnRfaWQiOiAiMTEwMDY3MTMyMjI2NDQyOTUzNDkzIiwKICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICJ0b2tlbl91cmkiOiAiaHR0cHM6Ly9vYXV0aDIuZ29vZ2xlYXBpcy5jb20vdG9rZW4iLAogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS83NDI5Mzc4NzUyOTAtY29tcHV0ZSU0MGRldmVsb3Blci5nc2VydmljZWFjY291bnQuY29tIiwKICAidW5pdmVyc2VfZG9tYWluIjogImdvb2dsZWFwaXMuY29tIgp9Cg=='
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['t1'])


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
    topic = request.json.get('topic', 't1')
    print("topic is ->", topic)
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