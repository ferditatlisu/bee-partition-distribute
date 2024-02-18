from typing import List

from kafka import KafkaConsumer
from src.configs.applicationconfig import ApplicationConfig
from src.configs.config import get_config
from src.services.kafkaconsumerservice import KafkaConsumerService
from src.services.kafkadminclientservice import KafkaAdminClientService
from src.services.kafkaproducerservice import KafkaProducerService
from kafka.structs import TopicPartition

class DistributeHandler():
    partition_index: int 
    ignored_partitions: List[int]
    max_partition_number: int
    kafka_producer: KafkaProducerService
    admin_client: KafkaAdminClientService
    consumer_service: KafkaConsumerService
    config: ApplicationConfig
    lag: int = 0
    next_index: int
    
    def __init__(self, kafka_admin_clint: KafkaAdminClientService, partition_index: int, max_partition_number: int):
        self.partition_index = int(partition_index)
        self.max_partition_number = max_partition_number
        self.config = get_config()
        self.ignored_partitions = self.config.ignored_partitions
        self.kafka_producer = KafkaProducerService(self.config.topic)
        self.admin_client = kafka_admin_clint
        self.next_index = 0
        
    def handle(self):
        topic_partitions: List[TopicPartition] = []
        topic_partitions.append(TopicPartition(self.config.topic, int(self.partition_index)))

        self.consumer_service = KafkaConsumerService()
        
        self.set_lag(topic_partitions)
        self.consumer_service.assign(topic_partitions)
        self.consumer_service.consume(self.event_handler)
        
    def event_handler(self, events):
        if len(events) == 0 and self.lag <= 0:
            self.consumer_service.stop()
            return
        
        for event in events:
            self.kafka_producer.publish(event, self.get_next_partition())
            self.increase_index()
            
        self.lag -= len(events)
        
    def get_next_partition(self):
        while True:
            founded = self.check_index()
            if not founded:
                break
        
        return self.next_index
          
    def check_index(self):
        for ignored_partition in self.ignored_partitions:
            if self.next_index == ignored_partition:
                self.increase_index()
                return True
        
        return False
    
    def increase_index(self):
        self.next_index += 1
        if self.next_index > self.max_partition_number:
            self.next_index = 0
            
    def set_lag(self, topic_partitions):
        group_offset = 0
        group_offsets = self.admin_client.get().list_consumer_group_offsets(self.config.group_id)
        for topic_partition, value in group_offsets.items():   
            if topic_partition.partition == self.partition_index:
                group_offset = value.offset
                break
        
        topic_offset = 0
        end_offsets = self.consumer_service.consumer.end_offsets(topic_partitions)
        if end_offsets:
            for topic_partition, offset in end_offsets.items():
                topic_offset = offset
                break
            
        lag = topic_offset - group_offset
        self.lag = lag