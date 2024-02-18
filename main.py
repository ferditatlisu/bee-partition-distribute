import time
from src.app.validationhandler import ValidationHandler
from src.configs.config import get_config
from src.services.redisservice import RedisService

def main():
    # time.sleep(15)
    redis = RedisService()
    redis.update_state_of_event(get_config().key, "Requested")
    handler = ValidationHandler()
    handler.handle()
    
main()