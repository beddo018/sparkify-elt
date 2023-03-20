import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''
The functions in this file execute their namesake grouped queries in 'sql_queries.py'.
Their primary purpose is to create the staging tables and the five denormalized tables
within the Redshift cluster created via psycopg2 Python shell for AWS.
'''

def drop_tables(cur, conn):

    '''
    Drops all the tables created by the subsequent create_tables function.
    Its placement allows the file to be run multiple times for experimentation purposes.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates the two staging tables and the five denormalized (star schema) OLAP tables.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Uses the variables created in 'dwh.cfg' to connect to the Redshift cluster, then invokes the above function 
    vis a vis the cursor object created by psycopg2. 
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    