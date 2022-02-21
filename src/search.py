#!/usr/bin/python
# Import third party package
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from pathlib import Path
import logging
import os

class Search:
    
    
    # So logs will be printed
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.debug("test")    
        
        
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        
        
    """
    Search for terms from the campaigns table based on the structure_value column
    """
    def get_campaigns(self, search_term: str) -> str:
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, self.dbname, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        seach_query = sql.SQL(''' SELECT DISTINCT structure_value FROM campaigns WHERE structure_value LIKE '%{}%' LIMIT 10 '''.format(search_term))
        cur.execute(seach_query)
        res = cur.fetchall()
        campaigns_search_term = ''
        if len(res) > 0:
            campaigns_search_terms = [searched[0] for searched in res]
            campaigns_search_term = campaigns_search_terms[0]
        search_terms_found = self.get_search_terms(search_term)
        search_terms_found.insert(0, campaigns_search_term)
        conn.close()
        return tuple(search_terms_found)
        
        
    """
    Search for terms from the adgroup table based on the alias column
    """
    def get_adgroups(self, search_term: str) -> str:
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, self.dbname, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        seach_query = sql.SQL(''' SELECT DISTINCT alias FROM adgroups WHERE alias LIKE '%{}%' LIMIT 10'''.format(search_term))
        cur.execute(seach_query)
        res = cur.fetchall()
        adgroups_search_term = ''
        if len(res) > 0:
            adgroups_search_terms = [searched[0] for searched in res]
            adgroups_search_term = adgroups_search_terms[0]
        search_terms_found = self.get_search_terms(search_term)
        search_terms_found.insert(0, adgroups_search_term)
        conn.close()
        return tuple(search_terms_found)
    
    
    """
    Search for terms from the adgroup table based on the alias column
    """
    def get_search_terms(self, search_term: str) -> str:
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, self.dbname, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        seach_query = sql.SQL(''' SELECT cost, conversion_value FROM search_terms WHERE search_term LIKE '%{}%' LIMIT 10 '''.format(search_term))
        cur.execute(seach_query)
        res = cur.fetchall()
        total_cost = 0.0
        total_conversion_value = 0.0
        for searched in res:
            total_cost += float(searched[0])
            total_conversion_value += float(searched[1])
        roas = 0.0 if round(total_cost, 1) == 0.0 else total_conversion_value/total_cost
        found = [search_term, round(total_cost, 1), round(total_conversion_value, 1), round(roas, 1)]
        conn.close()
        return found