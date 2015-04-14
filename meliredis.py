#!/home/mf/.virtualenvs/meli/bin/python
# -*- coding: utf-8 -*-

import importlib
import json

from bson import json_util
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
PAGE_LIMIT = 1 #10 #total pages to scrap


ALLOWED_CATEGORIES = {
    'MLA352542': 'iPhone 6 16gb',
    'MLA352543': 'iPhone 6 64gb',
    #'MLA352546': 'iPhone 6 128gb',
    'MLA119876': 'Samsung Galaxy 4',
    'MLA127623': 'Samsung Galaxy 5',
    'MLA351978': 'Moto G',
    #'MLA126252': 'Xperia Z2',
    #'MLA372245': 'Xperia',
}


rd = redis.StrictRedis(host='localhost', port=6379, db=1)
p = rd.pubsub()

def get_datetime(utc_diff=3): #argentina utc -3 
    return datetime.utcnow() - relativedelta(hours=utc_diff)

def extract_datetime(item_date):
    #eg '2015-03-03T23:18:11.000Z'
    #TODO: handle timezones
    return datetime.strptime(item_date, "%Y-%m-%dT%H:%M:%S.%fZ")


def get_rdid(item_type, item_id):
    return item_type + '-' + str(item_id)



class MeliCollector(): #make all into a class  
    def __init__(self):
        api_module = importlib.import_module('meli_api')
        self.mapi = getattr(api_module, 'MeliAPI')()
        self.sid = 'MLA'
       
    
    def started_today(self, item_date):
        diff = datetime.today() - extract_datetime(item_date)
        if diff.days == 0:
            return True
        return False


    def get_stats(self):
        stats = {}
        for category_id in ALLOWED_CATEGORIES:
            sold_quantity = int(eval(rd.get(get_rdid('categories', category_id))))
            stats[category_id] = [ALLOWED_CATEGORIES[category_id], sold_quantity]
        
        rd.publish('categories', json.dumps(stats))
        return stats
        
    
    def update_category(self, category_id, item_sold_today):
        redis_id = get_rdid('categories', category_id)
        in_redis = rd.get(redis_id)
        if not in_redis:
            rd.set(redis_id, item_sold_today)   
            
            #rd.publish('categories', {'category_id': category_id, 'sold_quantity': item_sold_today})

        else:
            sold_acum = int(eval(in_redis)) + item_sold_today
            rd.set(redis_id, sold_acum)
        
            #rd.publish('categories', {'category_id': category_id, 'sold_quantity': sold_acum})
        
       

    def insert_item(self, item, pn):
        redis_id = get_rdid('items', item['id'])
        in_redis = rd.get(redis_id)
        if not in_redis or in_redis == 'null': #TODO: ??
            #first time considering the item today
            #check if the item started selling today
            if self.started_today(item['start_time']):
                sold_today = item['sold_quantity']
                rd.set(redis_id, {'prev_sold_quantity': 0, 'sold_today':sold_today}) #TODO: add another fields
            else:
                #theres a flaw here, if the item didnt started selling today and its the first time the item appears
                sold_today = 0
                rd.set(redis_id, {'prev_sold_quantity':item['sold_quantity'],'sold_today':sold_today})
            
        else: #item already in redis, add sold_quantity diff
            prev_sold = eval(in_redis)['prev_sold_quantity']
            sold_today = item['sold_quantity'] - prev_sold
            print "item: %s, sold %s, started with %s" % (redis_id, sold_today, prev_sold)
            rd.set(redis_id, {'prev_sold_quantity':prev_sold,'sold_today':sold_today})
        
        self.update_category(item['category_id'], sold_today)
    
    
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
            
            

    def get_items(self, cat_id, limit=RES_LIMIT):
        """
        get all the items of a category.
        """
        offset = 0
        total_pages = PAGE_LIMIT
        for pn in range(total_pages):
            print pn
            items_data = self.mapi.search_by_category(cat_id, limit, offset)
            offset += int(limit)
                           
            items = items_data['results']        
            print "items amount: %d" % len(items)
            for item in items:
                self.insert_item(item, pn)

    

    def collect_categories(self, cat_ids):
            for catid in cat_ids:
                print catid
                self.get_items(catid)


def main(workers):
    jobs = []
    mc = MeliCollector()

    if workers != 1:
        while True:
            procs = []
            cats = ALLOWED_CATEGORIES.keys() #FIXME: send all cats to get_all_cats
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
        mc.collect_categories(['MLA126252'])



if __name__ == '__main__':
    if sys.argv[1] == 'test':
        main(workers=1)
    else:
        main(workers=multiprocessing.cpu_count() * 2)
