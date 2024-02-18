from datetime import datetime
import json
import os
from typing import List
from src.configs.applicationconfig import ApplicationConfig, KafkaConfig, RedisConfig

class ConfigurationManager:
    config: ApplicationConfig
    
    def __init__(self):
        self.config = ApplicationConfig()
        self.create_app_config()
        self.create_service_config()
        
        
    def create_app_config(self):
        self.config.ignored_partitions: List[int] = os.getenv("PARTITIONS").replace(" ", "").split(",") #check is int
        self.config.pod_name = os.getenv("POD_NAME")
        self.config.key = os.getenv("KEY")
        self.config.topic = os.getenv("TOPIC")
        self.config.group_id = os.getenv("GROUP_ID")
        self.config.time = datetime.now()
    
    def create_service_config(self):
        kafka_config = os.getenv("KAFKA_CONFIG")
        kafka_json = json.loads(kafka_config)
        self.config.kafka_config = KafkaConfig(kafka_json)
        
        pod_master_url = os.getenv("POD_MASTER_URL")
        self.config.master_url = pod_master_url
        
        redis_config = RedisConfig()
        redis_config.host = os.getenv("REDIS_HOST").replace(" ", "").split(",")
        redis_config.password = os.getenv("REDIS_PASSWORD")
        redis_config.port = os.getenv("REDIS_PORT")
        redis_config.master_name = os.getenv("REDIS_MASTERNAME")
        redis_config.database = os.getenv("REDIS_DB")
        self.config.redis = redis_config
        
        
config_manager = ConfigurationManager()
def get_config():
    return config_manager.config