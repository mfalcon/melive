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
PAGE_LIMIT = 4 #10 #total pages to scrap


ALLOWED_CATEGORIES = {
    #celulares
    'MLA352542': 'iPhone 6 16gb',
    'MLA352543': 'iPhone 6 64gb',
    #'MLA352546': 'iPhone 6 128gb',
    #'MLA': 'Samsung Galaxy 3',
    #'MLA119876': 'Samsung Galaxy 4',
    'MLA127623': 'Samsung Galaxy 5',
    'MLA351978': 'Moto G',
    #'MLA126252': 'Xperia Z2',
    #'MLA372245': 'Xperia',
    #computacion -> notebooks
    
    #'MLA13516': 'Acer',
    #'MLA13996': 'Apple',
    #'MLA51710': 'Asus',
    #'MLA32195': 'Bangho',
    #'MLA13517': 'Dell',
    #'MLA13517': 'HP',
    #'MLA13513': 'Lenovo',
    #'MLA83596': 'Samsung',
    #'MLA13514': 'Sony Vaio',
    #'MLA13524': 'Toshiba',
    
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


    def get_category_tree(self, category_id):
        #returns full category tree
        cat_data = self.mapi.get_category(category_id)
        cat_tree = {}
        cat_data['children_categories']
        cat_data['path_from_root']
        import pdb; pdb.set_trace()
        #for cat in cat_data[]:
            
        
        
    
    def get_target(self, category_id):
        '''
        get the desired category for the chart 
        '''
        if category_id in ALLOWED_CATEGORIES:
            return category_id
        else:
            cat_data = rd.get(category_id)
            import pdb; pdb.set_trace()
            cat_tree = cat_data
        


    def get_stats(self):
        stats = {}
        for category_id in ALLOWED_CATEGORIES:
            sold_quantity = int(eval(rd.get(category_id)))
            stats[category_id] = [ALLOWED_CATEGORIES[category_id], sold_quantity]
        
        rd.publish('categories', json.dumps(stats))
        rd.set('cats_stats', json.dumps(stats)) #FIXME: pretty inneficient
        return stats
        
    
    def update_category(self, category_id, item_sold_diff):
        in_redis = rd.get(category_id)
        #if the category is not in the ALLOWED_CATEGORIES dict, then
        #look in its tree to find a suitable one(parent/child)
        target_cat = self.get_target(category_id)
        
        if not in_redis or in_redis == 'null':
            #TODO: also set the category tree
            #cat_tree = self.get_category_tree(category_id)
            print "setting category: %s on redis" % category_id
            rd.set(category_id, item_sold_diff)   
            
            

        else:#F77070
            sold_acum = int(eval(in_redis)) + item_sold_diff #FIXME: wrong?
            rd.set(category_id, sold_acum)
        
        
       

    def insert_item(self, item, pn):
        in_redis = rd.get(item['id'])
        sold_today = 0
        sold_diff = 0
        if not in_redis or in_redis == 'null': #TODO: ??
            #first time considering the item today
            rd.set(item['id'], {'prev_sold_quantity':item['sold_quantity'],'sold_today':sold_today, 'sold_diff':sold_diff})
            
        else: #item already in redis, add sold_quantity diff
            item_redis = eval(in_redis)
            prev_sold = item_redis['prev_sold_quantity']
            sold_diff = item['sold_quantity'] - (item_redis['sold_today'] + prev_sold)
            sold_today = item['sold_quantity'] - prev_sold #updating sold_today
            
            rd.set(item['id'], {'prev_sold_quantity':prev_sold,'sold_today':sold_today,'sold_diff':sold_diff})
            '''
            if sold_diff:
                print "**** updating category ****"
                self.update_category(item['category_id'], sold_diff)
            '''
        self.update_category(item['category_id'], sold_diff) #this is using the item's category_id, which may differ from the desired allowed category
        
        
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
        #TODO: check if its convenient to set the category here
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
