from typing import Any, Dict, List
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from src.configs.config import get_config


class KafkaConsumerService():
    consumer: KafkaConsumer
    is_stopped:bool
    
    def __init__(self):
        self.is_stopped = False
        self.create_consumer()
       
    def stop(self):
        self.is_stopped = True
    
    def get_consumer(self):
        return self.consumer
    
    def create_consumer(self):
        config = get_config().kafka_config
        
        if config.user_name:
            self.consumer = KafkaConsumer(bootstrap_servers=config.host, 
                                      auto_offset_reset="earliest",
                                      consumer_timeout_ms=5000,
                                      ssl_check_hostname=True,
                                      enable_auto_commit=True,
                                      security_protocol="SASL_SSL",
                                      sasl_mechanism="SCRAM-SHA-512",
                                      sasl_plain_username=config.user_name,
                                      sasl_plain_password=config.password,
                                      ssl_cafile=f"./resources/{config.name}.pem",
                                      group_id=get_config().group_id)
        else:
            self.consumer = KafkaConsumer(bootstrap_servers=config.host, 
                                      auto_offset_reset="earliest",
                                      consumer_timeout_ms=5000,
                                      enable_auto_commit=True,
                                      group_id=get_config().group_id)
       
    def assign(self, partitions: List[TopicPartition]):
        self.consumer.assign(partitions)
            
    def consume(self, execute):
        max_date = get_config().time
        while True:
            kafka_items: Dict[Any, list] = self.consumer.poll(timeout_ms=1000, max_records=50)
            if self.is_stopped:
                break
            
            if len(kafka_items) == 0:
                execute([])
                continue

            events = []
            for key, value in kafka_items.items():
                for record in value:
                    if record.timestamp > max_date:
                        self.is_stopped = True
                    
                    events.append(record)
                    
            execute(events)