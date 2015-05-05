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
        #in_redis = rd.get(item['id'])
        in_redis = rd.hmget('sellers-%s' % seller_id, item['id'])
        
        if not in_redis[0]: #in_redis returns [None] ??
            #first time considering the item today
            prev_sold = item['sold_quantity']
            sold_today = 0
            
        else: #item already in redis, add sold_quantity diff
            item_redis = json.loads(in_redis)
            prev_sold = item_redis['prev_sold_quantity']
            sold_today = item['sold_quantity'] - prev_sold #updating sold_today
        
        #meli datetime example: 2015-04-30T20:00:00.000-03:00
        today_visits = self.mapi.get_items_visits([item['id']], self.today, time_point)

        item_data = {
                'prev_sold_quantity': prev_sold,
                'sold_today': sold_today,
                'today_visits': today_visits['total_visits'],
                'conversion-rate': float(today_visits/sold_today) if sold_today else 0.0,
                'title': item['title']
        }
        if sold_today:
            print "item %s sold %s units at a conversion rate of %s" % (sold_today, float(today_visits/sold_today) if sold_today else 0.0)
        #rd.set(item['id'], item_data)
        rd.hmset('sellers-%s' % seller_id, {'items-%s' % item['id']: item_data})


    def sellers_collector(self, queue):
        print os.getpid(),"working"
        while True:
            seller_id = queue.get(True)
            print os.getpid(), "got", seller_id
            print "getting items"
            time_point = datetime.isoformat(datetime.now()) #uniform datetime
            self.get_items(seller_id, time_point)
            rd.publish('sellers', 'sellers-%s' % seller_id)
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
            while True:
                for seller_id in sellers_id:
                    time_point = datetime.isoformat(datetime.now()) #uniform datetime
                    self.get_items(seller_id, time_point)
                    rd.publish('sellers', 'sellers-%s' % seller_id)
                

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
            sellers_q = multiprocessing.Queue()
            [sellers_q.put(cat) for cat in cats]
            [sellers_q.put('sentinel') for i in range(workers)]
            the_pool = multiprocessing.Pool(workers, mc.sellers_collector,(sellers_q,))
            the_pool.close()
            the_pool.join()
            

    else:
        mc.collect_sellers(['92607234'])



if __name__ == '__main__':
    if sys.argv[1] == 'test':
        main(workers=1)
    else:
        main(workers=multiprocessing.cpu_count() * 2)
