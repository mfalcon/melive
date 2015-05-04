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


rd = redis.StrictRedis(host='localhost', port=6379, db=2)
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
        today = datetime.today()
        self.today = datetime.isoformat(datetime(today.year,today.month,today.day))
      

    def insert_item(self, item, seller_id, time_point):
        sold_today = 0
        sold_diff = 0
        
        #in_redis = rd.get(item['id'])
        in_redis = rd.hmget('sellers-%s' % seller_id, item['id'])
        
        if not in_redis: # or in_redis == 'null': #TODO: ??
            #first time considering the item today
            prev_sold = item['sold_quantity']
            
        else: #item already in redis, add sold_quantity diff
            item_redis = json.loads(in_redis)
            prev_sold = item_redis['prev_sold_quantity']
            #FIXME: sometimes I get an -1 value
            sold_diff = item['sold_quantity'] - (item_redis['sold_today'] + prev_sold)
            sold_today = item['sold_quantity'] - prev_sold #updating sold_today

        
        #meli datetime example: 2015-04-30T20:00:00.000-03:00
        today_visits = self.mapi.get_items_visits([item['id']], self.today, time_point)
                    
        item_data = {
                'prev_sold_quantity': prev_sold,
                'sold_today': sold_today,
                'sold_diff': sold_diff,
                'today_visits': today_visits,
                'conversion-rate': today_visits/sold_today,
                'title': item['title']
        }
        
        #rd.set(item['id'], item_data)
        rd.hmset('sellers-%s' % seller_id, {'items-%s' % item['id']: item_data})
        rd.publish('sellers-%s' % seller_id)
        
    def sellers_collector(self, queue):
        print os.getpid(),"working"
        while True:
            seller_id = queue.get(True)
            print os.getpid(), "got", seller_id
            print "getting items"
            time_point = datetime.isoformat(datetime.now()) #uniform datetime
            self.get_items(seller_id, time_point)
            if catid == 'sentinel':
                print "breaking"
                break



    def get_items(self, seller_id, time_point, limit=RES_LIMIT):
        """
        get all the items of a seller.
        """
        offset = 0
        items_data = self.mapi.search_by_seller(seller_id, limit, offset)
        total_items = items_data['paging']['total']
        print "total items: %s" % total_items
        total_pages = total_items/items_data['paging']['limit'] + 1 #FIXME: RES_LIMIT not paging limit
        print "total pages: %s" % total_pages
        for pn in range(total_pages):
            print pn
            items_data = self.mapi.search_by_seller(seller_id, limit, offset)
            offset += int(limit)
            items = items_data['results']        
            if items:
                #separate into chunks and make a call to self.mapi.get_items_visits(ids_list, date_from, date_to)
                for item in items:
                    self.insert_item(item, seller_id, time_point) 
        
        

    

    def collect_sellers(self, sellers_id):
            for seller_id in sellers_id:
                self.get_items(seller_id)
                stats = self.publish_stats()
                print "publishing stats of seller %s" % seller_id
                print stats
                


def main(workers):
    jobs = []
    mc = MeliCollector()

    if workers != 1:
        sellers = SELLERS.keys()
        for seller in sellers:
            print "setting seller: %s" % seller
            rd.set('seller:%s' % seller, json.dumps({}))
            
        while True:
            procs = []
            cats_q = multiprocessing.Queue()
            [cats_q.put(cat) for cat in cats]
            [cats_q.put('sentinel') for i in range(workers)]
            the_pool = multiprocessing.Pool(workers, mc.sellers_collector,(cats_q,))
            the_pool.close()
            the_pool.join()

            print "batch finished"
            stats = mc.get_stats()
            print stats
            

    else:
        mc.collect_sellers(['134137537'])



if __name__ == '__main__':
    if sys.argv[1] == 'test':
        main(workers=1)
    else:
        main(workers=multiprocessing.cpu_count() * 2)
