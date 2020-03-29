import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
        Iterate over all the drop table queries and execute them
        
        INPUTS:
        * the cursor variable of the database (cur)
        * the connection variable of the database (conn)
    """
    
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
        Iterate over all the create table queries and execute them
        
        INPUTS:
        * the cursor variable of the database (cur)
        * the connection variable of the database (conn)
    """
        
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    
    """
        Connect to the database using credentials in the config file and then drop
        and recreate the required tables
    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Dropping tables.')
    drop_tables(cur, conn)
    
    print('Tables dropped. Creating new tables.')
    create_tables(cur, conn)
    print('Tables created.')

    conn.close()


if __name__ == "__main__":
    main()