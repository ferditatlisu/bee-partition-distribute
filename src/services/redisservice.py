import json
from redis import Redis, SentinelConnectionPool
from redis.sentinel import Sentinel

from src.configs.config import get_config


class RedisService:
    redis_connection : Redis
    
    def __init__(self):
        sentinel = Sentinel(self.__get_redis_sentinel_hosts())
        redis_config = get_config().redis
        pool = SentinelConnectionPool(service_name=redis_config.master_name, sentinel_manager=sentinel,
                                      password=redis_config.password,
                                      db=redis_config.database)


        self.redis_connection = Redis(connection_pool=pool)
        
    def __get_redis_sentinel_hosts(self):
        redis_config = get_config().redis
        redis_hosts = []
        redis_port = redis_config.port
        config_redis_hosts = redis_config.host
        for host in config_redis_hosts:
            redis_hosts.append((host, redis_port))
        return redis_hosts


    def get(self, key):
        res = self.redis_connection.get(key)
        if res:
            return json.loads(res)
        
        return None
    
    def set(self, key, value):
        redis_value = json.dumps(value)
        self.redis_connection.set(key, redis_value)
        
        
    def get_state_of_event(self, key: str):
        result = self.redis_connection.hmget(key, "status")
        return result[0]
    
    def update_state_of_event(self, key: str, state: str):
        self.redis_connection.hmset(key, {"status": state})
        
    def error_of_event(self, key: str, message: str):
        self.redis_connection.hmset(key, {"error": message, "status": "Error"})