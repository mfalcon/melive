#a kind of python meli api
import json
import logging
import time

import requests

BASE_URL = 'https://api.mercadolibre.com/'
BASE_SITE_URL = BASE_URL + 'sites/'
SLEEP_TIME = 0.05 #in seconds
ERROR_SLEEP_TIME = 0.1



class MeliAPI():
    def __init__(self, sid='MLA'):
        self.sid = sid
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        # create a file handler
        handler = logging.FileHandler('logs/mapi.log')
        handler.setLevel(logging.INFO)
        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(handler)
        self.logger = logger
        
    
    #TODO: replace it with requests retry
    def make_call(self, url, params=None):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:24.0) Gecko/20100101 Firefox/24.0'}
        time.sleep(SLEEP_TIME)
        for i in range(10):
            if i != 0:
                self.logger.info("%s - retrying... %d" % (url,i))
                time.sleep(ERROR_SLEEP_TIME*i)
            try:
                res = requests.get(url, headers=headers)
            except requests.ConnectionError, e:
                self.logger.info(e)
                continue
            
            if res.status_code == 200:
                data = json.loads(res.text)
                return data
            continue
                    
    '''
    def make_call(self, url):
        time.sleep(SLEEP_TIME)
        reqs_session = requests.Session()
        reqs_adapter = requests.adapters.HTTPAdapter(max_retries=5)
        reqs_session.mount('http://', reqs_adapter)
        reqs_session.mount('https://', reqs_adapter)
        res = reqs_session.get(url)
        if res.status_code == 200:
            data = json.loads(res.text)
            return data
        return None
    '''
    
    def get_seller_info(self, seller_id):
        url = BASE_URL + 'users/%s' % seller_id
        self.logger.info(url)
        data = self.make_call(url)
        return data
    
    
    def get_items_visits(self, ids_list, date_from, date_to): #bulk results
        #https://api.mercadolibre.com/items/{Items_id}/visits?date_from=2014-06-01T00:00:00.000-00:00&date_to=2014-06-10T00:00:00.000-00:00'
        url = BASE_URL + 'items/%s/visits?&date_from=%s&date_to=%s' % (",".join(ids_list), date_from, date_to)
        print url
        self.logger.info(url)
        data = self.make_call(url)
        return data
        
        
    def get_items_data(self, items_ids):
        #Retrieves the information of a list of items: GET/items?ids=:ids
        url = BASE_URL + 'items/?ids=%s' % ",".join(items_ids)
        self.logger.info(url[:50])
        data = self.make_call(url)
        return data


    def search_by_category(self, cat_id, limit, offset):
        #get the category items
        url = BASE_SITE_URL + '%s/search?category=%s&limit=%s&offset=%s&condition=new' % (self.sid, cat_id, limit, offset)
        self.logger.info(url)
        data = self.make_call(url)
        return data


    def search_by_seller(self, seller_id, limit, offset):
        #get the category items
        url = BASE_SITE_URL + '%s/search?seller_id=%s&limit=%s&offset=%s&condition=new' % (self.sid, seller_id, limit, offset)
        self.logger.info(url)
        data = self.make_call(url)
        return data


    def search_item(self, query):
        url = BASE_SITE_URL + '%s/search?q=%s' % (self.sid, query)
        data = self.make_call(url)
        return data


    def get_category(self, cat_id):
        """
        get category info
        """
        url = BASE_URL + 'categories/%s' % cat_id
        data = self.make_call(url)
        return data
