
import time
from src.app.validationhandler import ValidationHandler
from src.configs.applicationconfig import ApplicationConfig
from src.configs.config import ConfigurationManager, get_config
from src.services.redisservice import RedisService


def main():
    time.sleep(15)
    handler = ValidationHandler()
    handler.handle()
    
main()