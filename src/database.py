#!/usr/bin/python
# Import third party package
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from pathlib import Path
import logging
import os

class Database:
    
    
    # So logs will be printed
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.debug("test")
    
    # global varibles
    csv_save_path = Path('/home/log/Documents/bidnamic/data/') # folder where the CSV files are located.
                                     # NB: Provide the absolute path of the CSVs folder
    
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        
        
    """
    Verify if the targeted database exist
    """
    def exist(self, db_name: str) -> bool:
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, self.dbname, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        # should return True if the database exist or False otherwise
        list_databases_query = sql.SQL(''' SELECT datname FROM pg_database ''')
        cur.execute(list_databases_query)
        res = cur.fetchall()
        databases = [database[0] for database in res]
        if db_name in databases:
            conn.close()
            return True
        else:
            conn.close()
            return False
        
        
    """
    Create the database if not existing
    """
    def create(self, db_name: str) -> None:
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, self.dbname, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the curso
        if self.exist(db_name):
            conn.close()
            logging.info('Database: "' + db_name + '" already exists')
            return None
        else:
            # Use the psycopg2.sql module instead of string concatenation 
            # in order to avoid sql injection attacks.
            create_database_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
            cur.execute(create_database_query)
            conn.close()
            logging.info('Database: "' + db_name + '" created')
            return None
        
        
    """
    Create the table schema if not existing
    """
    def initalize_tables(self, db_name)-> None:
        if not self.exist(db_name):
            raise Exception('The given database name: "{0}" does not exist. \nPlease first create the database "{1}"'.format(db_name, db_name))
            return None
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, db_name, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        # create table schema here
        dirs = os.listdir(self.csv_save_path)
        for file in dirs:
            file_split = file.split('.')
            table = ''
            if file_split[1].lower() == 'csv':
                table = file_split[0]
                create_table_query = ''' CREATE TABLE IF NOT EXISTS %s (''' % (table)
                table_attributes_values = pd.read_csv(os.path.join(self.csv_save_path, file))
                table_attributes = ['%s text' % col for col in table_attributes_values.columns]
                for table_attribute in table_attributes:
                    if table_attributes.index(table_attribute) == len(table_attributes) - 1:
                        create_table_query += table_attribute
                    else:
                        create_table_query += table_attribute + ','
                create_table_query += ')'
                logging.info(create_table_query)
                cur.execute(create_table_query)
        conn.close()
        return None
    
    
    """
    Load/push the CSVs data into the database
    """
    def load_csv(self, db_name)-> None:
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, db_name, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        dirs = os.listdir(self.csv_save_path)
        for file in dirs:
            file_split = file.split('.')
            table = ''
            if file_split[1].lower() == 'csv':
                table = file_split[0]
                push_data_query = '''COPY %s FROM '%s' DELIMITER ',' CSV HEADER''' % (table, os.path.join(self.csv_save_path, file))
                cur.execute(push_data_query)
        conn.close()
        return None


    """
    Query util
    """
    def run_query(self, db_name, query):
        conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (self.host, db_name, self.user, self.password)) # connection to Postgres SQL db
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor() # Get the cursor
        list_query = '''%s''' % query
        cur.execute(list_query)
        res = cur.fetchall()
        list_res = [result[0] for result in res]
        conn.close()
        return list_res