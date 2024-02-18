from typing import Any, Dict, List
from kafka import KafkaProducer
from src.configs.config import get_config


class KafkaProducerService():
    producer: KafkaProducer
    topic_name: str
    def __init__(self, topic_name:str):
        self.topic_name = topic_name
        self.create()
    
    def get(self):
        return self.producer
    
    def create(self):
        config = get_config().kafka_config
        
        if config.user_name:
            self.producer = KafkaProducer(bootstrap_servers=config.host, 
                                      security_protocol="SASL_SSL",
                                      sasl_mechanism="SCRAM-SHA-512",
                                      sasl_plain_username=config.user_name,
                                      sasl_plain_password=config.password,
                                      ssl_cafile=f"./resources/{config.name}.pem")
        else:
            self.producer = KafkaProducer(bootstrap_servers=config.host)
       
       
    def publish(self, event, partition):
        try:
            self.producer.send(topic=self.topic_name, value=event.value, key= event.key, headers= event.headers, partition= partition)
        except Exception as ex:
            print(ex)
  