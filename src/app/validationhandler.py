from src.app.partitionhandler import PartitionHandler
from src.configs.applicationconfig import ApplicationConfig
from src.configs.config import get_config
from src.services.httpclientservice import HttpClientService
from src.services.kafkadminclientservice import KafkaAdminClientService
from src.services.redisservice import RedisService


class ValidationHandler():
    redis: RedisService
    config: ApplicationConfig
    kafka_admin_client: KafkaAdminClientService
    
    def __init__(self):
        self.redis = RedisService()
        self.config = get_config()
        self.kafka_admin_client = KafkaAdminClientService()
    
    def handle(self):
        state = self.redis.get_state_of_event(self.config.key)
        if not state:
            self.stop_process("state is empty")
            return
        
        if state != b'Requested':
            self.stop_process("Not requested")
            return
        
        self.redis.update_state_of_event(self.config.key, "Created")
        topic = self.kafka_admin_client.get().describe_topics([self.config.topic])
        if len(topic[0].get("partitions", 0)) == 0:
            self.stop_process("Topic doesn't exist")
            return
        
        handler = PartitionHandler(self.kafka_admin_client)
        handler.handle()
        
        self.redis.update_state_of_event(self.config.key, "Completed")
        self.stop_process(None)
        
    def stop_process(self, message: str | None):
        if message:
            self.redis.error_of_event(self.config.key, message)
            print(message)
            
        http = HttpClientService()
        http.delete_pod()