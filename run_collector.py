# -*- coding: utf-8 -*-
#at a given time (12am GMT -3) run this script
#insert redis categories data in postgres relational db and then flush it
import datetime

import psycopg2
import redis

rd = redis.StrictRedis(host='localhost', port=6379, db=1)
cats_stats = rd.get('cats_stats')
if cats_stats:
    conn = psycopg2.connect("dbname=meli_stats user=mfalcon")
    cur = conn.cursor()
    for cat_id, values in eval(cats_stats).items():
        cur.execute("""INSERT INTO categories_stats (category_id, stats_date, name, units_sold, income, visits)
                    VALUES (%s, %s, %s, %s, %s, %s);""",
                    (cat_id, datetime.date.today(), values[0],values[1], 0, 0))
        conn.commit()
    
    # Close communication with the database
    cur.close()
    conn.close()
    
    #clear redis db
    rd.flushdb()

