"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)
logger.addHandler(c_handler)

# KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
# KAFKA_CONNECT_URL = 'http://172.19.0.6'
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logger.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")

    if resp.status_code == 200:
        logger.debug("connector already created skipping recreation")
        return
    else:
        logger.debug(f'Get request returned status code: {resp.status_code}')

    logger.debug('Making connection creation request')

    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://localhost:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": "cta.connect.",
               "poll.interval.ms": 3600000,
           }
       }),
    )

    ## Ensure a healthy response was given
    resp.raise_for_status()
    logger.debug("connector created successfully")
    return

if __name__ == "__main__":
    configure_connector()
