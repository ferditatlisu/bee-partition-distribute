import threading
from typing import List
from src.app.distributehandler import DistributeHandler
from src.configs.applicationconfig import ApplicationConfig
from src.configs.config import get_config
from src.services.kafkadminclientservice import KafkaAdminClientService
from src.services.kafkaconsumerservice import KafkaConsumerService
from kafka.structs import TopicPartition

class PartitionHandler():
    config: ApplicationConfig
    admin_client: KafkaAdminClientService
    
    def __init__(self, kafka_admin_client: KafkaAdminClientService):
        self.config = get_config()
        self.admin_client = kafka_admin_client
    
    def handle(self):
        max_partition_number = self.get_max_partition_number()        
        threads: List[threading.Thread] = []
        for ignored_partition in self.config.ignored_partitions:
            t = threading.Thread(target=self.execute, args= (ignored_partition, max_partition_number))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def execute(self, ignored_partition, max_partition_number):
        handler = DistributeHandler(self.admin_client, ignored_partition, max_partition_number)
        handler.handle()
        

    def get_max_partition_number(self):
        topics = self.admin_client.get().describe_topics([self.config.topic])
        max_partition_number = 0 
        for partition_meta_data in topics[0]["partitions"]:
            partition = partition_meta_data["partition"]
            if partition > max_partition_number:
                max_partition_number = partition
                
        return max_partition_number