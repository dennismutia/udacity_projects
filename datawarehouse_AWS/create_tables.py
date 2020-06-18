import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function deletes ETL tables if they exist before recreating them.
    
    Arguments:
        cur: the cursor object
        conn: the connection string to the DB
        
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates ETL staging tables and DWH fact and dimensional tables
    
    Arguments:
        cur: the cursor object
        conn: the connection string to the DB
        
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function calls the drop_tables and create_tables functions created above.
    
    Arguments:
        None
        
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()