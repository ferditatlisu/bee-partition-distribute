import time
from src.app.validationhandler import ValidationHandler

def main():
    time.sleep(10)
    handler = ValidationHandler()
    handler.handle()
    
main()