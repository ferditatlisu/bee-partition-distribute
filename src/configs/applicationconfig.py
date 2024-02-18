from datetime import datetime
from typing import Dict, List

class RedisConfig:
    host: List[str]
    port: int | str
    password: str
    database: int | str
    master_name: str
    
    
class KafkaConfig:
    id : int
    name :str
    host: str
    user_name: None | str
    password: None | str
    certificate: None | List[str]
    
    
    def __init__(self, dict_list: List[Dict], cluster_id:int):
        as_dict: Dict = {}
        for kafka_config in dict_list:
            if kafka_config["id"] == cluster_id:
                as_dict = kafka_config
                break
    
        self.id = as_dict["id"]
        self.name = as_dict["name"]
        self.host = as_dict["host"]
        self.user_name = as_dict.get("userName", None)
        self.password = as_dict.get("password", None)
        self.certificate = as_dict.get("certificate", None)
        
        if self.certificate is None:
            return
        
        # open(f"resources/{self.name}.pem", "x")
        
        with open(f"resources/{self.name}.pem", "w") as txt_file:
                for line in self.certificate:
                    txt_file.write(line + "\n")
        

class ApplicationConfig:
    master_url: str
    redis : RedisConfig
    kafka_config: KafkaConfig
    ignored_partitions: List[int]
    pod_name: str
    key: str
    topic: str
    group_id: str
    time: int