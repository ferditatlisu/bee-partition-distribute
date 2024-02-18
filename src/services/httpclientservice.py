import urllib3

from src.configs.config import get_config


class HttpClientService():
    http: urllib3.PoolManager
    url:str
    pod_name: str
    def __init__(self):
        retries = urllib3.Retry(connect=5, read=2, redirect=5)
        self.http = urllib3.PoolManager(retries = retries)
        self.url = get_config().master_url
        self.pod_name = get_config().pod_name
    
    def delete_pod(self):    
        try:
            _ = self.http.request(
                "DELETE", 
                f'{self.url}/master?key={self.pod_name}', 
                headers={'Connection': 'close'})
        except Exception as ex:
            print(ex)