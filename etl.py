import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
        Iterate over the list of load queries that load the data from
        files in s3 bucket to the stage tables using COPY command
         
        INPUTS:
        * the cursor variable of the database (cur)
        * the connection variable of the database (conn)
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
        Iterate over the list of insert queries that insert the data from stage table to final table
        
        INPUTS:
        * the cursor variable of the database (cur)
        * the connection variable of the database (conn)
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
        Entry point of the program.
        
        Read the database credentials from the config file, connects to the database, 
        loads the S3 files into sage tables, loads the final tables from stage tables
        and optionally closes the database connection if needed
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()