# -*- coding: utf-8 -*-
#collect meli stats, runs without interruptions

import importlib
import json

import os
import sys
import time
import timeit
import logging
import math
import multiprocessing
from datetime import datetime
from random import shuffle, randint

from dateutil import relativedelta
import redis 

import meli_api


INITIAL_OFFSET = 0
RES_LIMIT = 200
INITIAL_PAGE_LIMIT = 5
PAGE_LIMIT = 5 #10 #total pages to scrap
ITEMS_IDS_LIMIT = 50 #bulk items get limit


SELLERS = {
    '134137537': 'TEKNOKINGS',
}


rd = redis.StrictRedis(host='localhost', port=6379, db=1)
p = rd.pubsub()

def get_datetime(utc_diff=3): #argentina utc -3 
    return datetime.utcnow() - relativedelta(hours=utc_diff)


def extract_datetime(item_date):
    #eg '2015-03-03T23:18:11.000Z'
    #TODO: handle timezones
    return datetime.strptime(item_date, "%Y-%m-%dT%H:%M:%S.%fZ")



class MeliCollector(): #make all into a class  
    def __init__(self):
        api_module = importlib.import_module('meli_api')
        self.mapi = getattr(api_module, 'MeliAPI')()
        self.sid = 'MLA'


    def get_stats(self):
        stats = {}
        for category_id in ALLOWED_CATEGORIES:
            cat_data = rd.get(category_id)
            print category_id
            print cat_data
            data = eval(cat_data)
            sold_quantity = data['total_sold']
            stats[category_id] = [ALLOWED_CATEGORIES[category_id], sold_quantity]
        
        rd.publish('categories', json.dumps(stats))
        rd.set('cats_stats', json.dumps(stats)) #FIXME: pretty inneficient
        return stats
       

    def insert_item(self, item, seller_id):
        in_redis = rd.get(item['id'])
        sold_today = 0
        sold_diff = 0
        if not in_redis or in_redis == 'null': #TODO: ??
            #first time considering the item today
            rd.set(item['id'], {'prev_sold_quantity': item['sold_quantity'],'sold_today':sold_today, 'sold_diff':sold_diff})
            
        else: #item already in redis, add sold_quantity diff
            item_redis = eval(in_redis)
            prev_sold = item_redis['prev_sold_quantity']
            #FIXME: sometimes I get an -1 value
            sold_diff = item['sold_quantity'] - (item_redis['sold_today'] + prev_sold)
            sold_today = item['sold_quantity'] - prev_sold #updating sold_today
            
            rd.set(item['id'], {'prev_sold_quantity':prev_sold,'sold_today':sold_today,'sold_diff':sold_diff})
        
        
    def cats_collector(self, queue):
        print os.getpid(),"working"
        while True:
            catid = queue.get(True)
            print os.getpid(), "got", catid
            print "getting items"
            self.get_items(catid)
            if catid == 'sentinel':
                print "breaking"
                break



    def get_items(self, seller_id, limit=RES_LIMIT):
        """
        get all the items of a seller.
        """
        offset = 0
        items_data = self.mapi.search_by_seller(seller_id, limit, offset)
        total_items = items_data['paging']['total']
        print "total items: %s" % total_items
        total_pages = total_items/items_data['paging']['limit'] #FIXME: RES_LIMIT not paging limit
        print "total pages: %s" % total_pages

        for pn in range(total_pages):
            print pn
            items_data = self.mapi.search_by_seller(seller_id, limit, offset)
            offset += int(limit)
            items = items_data['results']        
            if items:
                for item in items:
                    self.insert_item(item, seller_id) 

    

    def collect_sellers(self, sellers_id):
            for seller_id in sellers_id:
                self.get_items(seller_id)
                


def main(workers):
    jobs = []
    mc = MeliCollector()

    if workers != 1:
        sellers = SELLERS.keys()
        for cat_id in cats:
            print "setting category: %s" % cat_id
            rd.set(cat_id, {'total_sold': 0})
            
        while True:
            procs = []
            cats_q = multiprocessing.Queue()
            [cats_q.put(cat) for cat in cats]
            [cats_q.put('sentinel') for i in range(workers)]
            the_pool = multiprocessing.Pool(workers, mc.cats_collector,(cats_q,))
            the_pool.close()
            the_pool.join()

            print "batch finished"
            stats = mc.get_stats()
            print stats
            

    else:
        mc.collect_sellers(['MLA13516'])



if __name__ == '__main__':
    if sys.argv[1] == 'test':
        main(workers=1)
    else:
        main(workers=multiprocessing.cpu_count() * 2)
