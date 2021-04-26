"""Defines core consumer functionality"""
import logging

import asyncio
import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = 'PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094'

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=1.0,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            # 'schema.registry.url': SCHEMA_REGISTRY_URL,
            'bootstrap.servers': BROKER_URL,
            'group.id':'0',
            'auto.offset.reset':'earliest' if self.offset_earliest else 'latest'
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(
                self.broker_properties,
                )
        else:
            self.consumer = Consumer(
                self.broker_properties,
            )

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name_pattern], on_assign = self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                logger.info(f'Attempting to consume for {self.topic_name_pattern}')
                num_results = self._consume()
                logger.info(f'n results = {num_results}')
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        logger.info(f'Inside _consume fn')
        try:
            message = self.consumer.poll(self.consume_timeout)
        except Exception as e:
            logger.info(f'Error while polling for {self.consumer}: {e}')
        logger.info(f'This side of try/except: {message}')
        logger.info(f'{message is None}')
        if True:
            logging.info(f'No message found while polling for {self.consumer}')
            return 0
        elif message.error:
            logger.info(f'Error while consuming for {self.consumer}')
        else:
            logger.info(f'Message consumed while polling for {self.consumer}: message.value()')
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""

        self.consumer.close()
