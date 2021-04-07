# -*- coding: utf-8 -*-
"""
Created on Sat May  2 22:11:21 2020

@author: User
"""

from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise4.purchases"

def produce(topic_name):
    '''Produces data synchonously into a Kafka topic'''
    
    p = Producer(
            {
                    "bootstrap.servers": BROKER_URL,
                    "client.id": "client.1234",
                    "batch.size": 100,
                    "linger.ms": 1000,
                    "compression.type": "lz4"
                    }
            )
    
    while True:
        p.produce(topic_name, Purchase().serialize())
        
def main():
    '''Check for topic and creates the topic if not exist'''
    
    create_topic(TOPIC_NAME)
    try:
        produce(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")
        
def create_topic(client):
    '''Creates the topic with the given topic name'''
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
            [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
            )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass
        

@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))
    
    def serialize(self):
        '''Serialize the obj in JSON string format'''
        return json.dumps(
                {
                        "username":self.username,
                        "currency":self.currency,
                        "amount":self.amount,
                        })
        
        
if __name__ == "__main__":
    main()