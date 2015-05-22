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
ITEMS_IDS_LIMIT = 50 #bulk items/visits get limit


SELLERS = {
    '134137537': 'TEKNOKINGS',
    '92607234': 'DATA TOTAL',
    '5846919' : 'COMPUDATASOFT',
    '80183917' : 'GEZATEK COMPUTACION',
}


rd = redis.StrictRedis(host='localhost', port=6379, db=2)
p = rd.pubsub()


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


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

    
    def format_data(self, seller_id):
        data = []
        items =  rd.hgetall('sellers-%s' % seller_id)
        for item_id in items.keys():
            item_data = eval(items[item_id])
            data.append(item_data)
        
        return json.dumps({'seller': SELLERS[seller_id], 'items': data})
        


    def insert_item(self, item, seller_id, today_visits):
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
        #today_visits = self.mapi.get_items_visits([item['id']], self.today, time_point)

        item_data = {
                'prev_sold_quantity': prev_sold,
                'sold_today': sold_today,
                'today_visits': today_visits,
                'conversion-rate': float(today_visits/sold_today) if sold_today else 0.0,
                'title': item['title']
        }
       
        #rd.set(item['id'], item_data)
        rd.hmset('sellers-%s' % seller_id, {'items-%s' % item['id']: item_data})


    def sellers_collector(self, queue):
        print os.getpid(),"working"
        while True:
            seller_page = queue.get(True)
            #print os.getpid(), "got seller: %s" % seller_page['seller_id']
            if seller_page == 'sentinel':
                print "breaking"
                break
            
            print "getting items"
            time_point = datetime.isoformat(datetime.now()) #uniform datetime
            self.get_items(seller_page, time_point)
            rd.publish('sellers', rd.hgetall('sellers-%s' % seller_page['seller_id']))
            


    def get_items(self, seller_page, time_point):
        """
        get all the items of a seller in the desired page.
        """
        print "seller page *******"
        print seller_page
        items_data = self.mapi.search_by_seller(seller_page['seller_id'], seller_page['limit'], seller_page['offset'])
        items = items_data['results']       
        if items:
            #separate into chunks and make a call to self.mapi.get_items_visits(ids_list, date_from, date_to)
            item_ids = [item['id'] for item in items]
            item_chunks = chunks(item_ids, ITEMS_IDS_LIMIT)
            items_visits = {}
            for item_ids in item_chunks:
                visits = self.mapi.get_items_visits(item_ids, self.today, time_point)
                for item_visits in visits:
                    items_visits[item_visits['item_id']] = item_visits['total_visits']
                
            for item in items:
                print item['title']
                today_visits = items_visits[item['id']]
                self.insert_item(item, seller_page['seller_id'], today_visits) 

    
    def get_pages(self, seller_ids, limit=RES_LIMIT):
        '''receives a seller_id and returns a list of page items
        to queue'''
        pages = []
        for seller_id in seller_ids:
            offset = 0
            items_data = self.mapi.search_by_seller(seller_id, limit, offset)
            total_items = items_data['paging']['total']
            print "total items: %s" % total_items
            total_pages = total_items/items_data['paging']['limit'] + 1 #FIXME: RES_LIMIT not paging limit
            print "total pages: %s" % total_pages
            for pn in range(total_pages):
                offset += int(limit)
                pages.append({'seller_id': seller_id, 'limit': limit, 'offset': offset})
        
        return pages


    def collect_sellers(self, seller_pages):
            while True:
                time_point = datetime.isoformat(datetime.now()) #uniform datetime
                for seller_page in seller_pages:
                    self.get_items(seller_page, time_point)
                    rd.publish('sellers', self.format_data(seller_page['seller_id']))
                

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
            pages = mc.get_pages(sellers)
            [sellers_q.put(page) for page in pages]
            [sellers_q.put('sentinel') for i in range(workers)]
            the_pool = multiprocessing.Pool(workers, mc.sellers_collector,(sellers_q,))
            the_pool.close()
            the_pool.join()
            

    else:
        pages = mc.get_pages(['80183917'])
        mc.collect_sellers(pages)



if __name__ == '__main__':
    if sys.argv[1] == 'test':
        main(workers=1)
    else:
        main(workers=multiprocessing.cpu_count() * 2)
