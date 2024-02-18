from kafka import KafkaAdminClient

from src.configs.config import get_config


class KafkaAdminClientService():
    admin_client: KafkaAdminClient
    
    def __init__(self):
        self.create_client()
        
    def get(self):
        return self.admin_client 
        
    def create_client(self):
        config = get_config().kafka_config
        
        if config.user_name:
            self.admin_client = KafkaAdminClient(
                            bootstrap_servers=config.host,
                            ssl_check_hostname=True,
                            security_protocol="SASL_SSL",
                            sasl_mechanism="SCRAM-SHA-512",
                            sasl_plain_username=config.user_name,
                            sasl_plain_password=config.password,
                            ssl_cafile=f"./resources/{config.name}.pem")
        else:
            self.admin_client = KafkaAdminClient(
                            bootstrap_servers=config.host)
