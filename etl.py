import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries

'''
The functions in this file invoke their namesake clustered functions in 'sql_queries.py' in succession.
Their primary purpose is to load the staging tables with data from Sparkify's logs and song metadata, then transform the
staged data into OLAP-query ready data in the denormalized star schema tables. 
'''

def load_staging_tables(cur, conn):
    '''
    Loops over the SQL COPY commands in 'sql_queries.py' to load the staging tables with Sparkify's log data and song metadata.
    '''
    for query in copy_table_queries:
        start = time.time()
        print(query)
        print('Started at {}'.format(start))
        cur.execute(query)
        conn.commit()
        end = time.time()
        print('Completed at {}'.format(end))
        print('Duration = {}'.format(end - start))


def insert_tables(cur, conn):
    '''
    Loops over the SQL INSERT commands in 'sql_queries.py' to finalize the ETL process by moving/transforming the staged
    song and log data into OLAP-query-ready tables.
    '''
    for query in insert_table_queries:
        start = time.time()
        print(query)
        print('Started at {}'.format(start))
        cur.execute(query)
        conn.commit()
        end = time.time()
        print('Completed at {}'.format(end))
        print('Duration = {}'.format(end - start))


def main():
    '''
    Executes the above functions vis a vis the Psycopg2 Python-based AWS Redshift cursor/connection, 
    then closes the connection to conserve resources.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
