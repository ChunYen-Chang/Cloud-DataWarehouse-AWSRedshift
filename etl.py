# IMPORT PACKAGES
import configparser
import psycopg2
from sql_queries import schema_set_path_query
from sql_queries import timetemp_table_create, copy_table_queries, insert_table_queries



# DEFINE FUNCTIONS
def load_staging_tables(cur, conn):
    """This function is about moving data from Amazon S3 to Amazon Redshift staging table. To
    achieve this, it exetracts SQL syntax information from copy_table_queries and execute the
    SQL syntax by cur.execute().
    """
    for query in copy_table_queries:
        try:
            print("execute query: " + query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def insert_tables(cur, conn):
    """This function is about moving data from staging table to analytical table. To achieve 
    this, it exetracts SQL syntax information from insert_table_queries and execute the SQL 
    syntax by cur.execute().
    """
    for query in insert_table_queries:
        try:
            print("execute query: " + query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def main():
    """This function is for "copying data from s3 to staging tables" and "moving data from
    staging tables to analytical tables". It can be seperated into six parts. First part 
    is using configparser package to read the configuration file(dwh.cfg) and get the variable
    which is needed for connectting to Redshift. Second part is about connectting to Redshift. 
    Third part is relating to copying data from s3 to staging tables. Fourth part is about 
    creating a "timetemp table" for temporary storing the transformed ts data (in timestamp
    format). The timetemp table will be used for following stage(inserting data to analytical 
    table, which is the time table). Fifth part is about moving data from staging tables to 
    analytical tables. Six part is about closing the connection.
    """
    # use configparser package to read the configuration file(dwh.cfg) and get needed variables
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_CLUSTER_HOST = config.get("CLUSTER","HOST")
    DWH_CLUSTER_DB_NAME = config.get("CLUSTER","DB_NAME")
    DWH_CLUSTER_DB_USER = config.get("CLUSTER","DB_USER")
    DWH_CLUSTER_DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
    DWH_CLUSTER_PORT = config.get("CLUSTER","DB_PORT")

    # connect to redshift    
    try:
        print("1. Create a connection") 
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                .format(DWH_CLUSTER_HOST, DWH_CLUSTER_DB_NAME, DWH_CLUSTER_DB_USER,
                                        DWH_CLUSTER_DB_PASSWORD, DWH_CLUSTER_PORT))
        cur = conn.cursor()
    except Exception as e:
        print(e)
    
    # set path to this schema
    cur.execute(schema_set_path_query)
    
    # copy data from S3 to staging tables
    print("2. copy data from S3 to staging tables")
    load_staging_tables(cur, conn)
    
    # create timetemp tables for following process (inserting data into time table)
    print("3. Create a timetemp table")
    cur.execute(timetemp_table_create)
    
    # move data from staging tables to analytical tables
    print("4. move data from staging tables to analytical tables")
    insert_tables(cur, conn)

    # close the connection
    print("5. close the connection")
    conn.close()


if __name__ == "__main__":
    main()