# IMPORT PACKAGES
import configparser
import psycopg2
from sql_queries import schema_create_query, schema_set_path_query, create_staging_table_queries
from sql_queries import create_analytical_table_queries, drop_table_queries


# DEFINE FUNCTIONS
def drop_tables(cur, conn):
    """This function is for dropping tables in Redshift (if these tables are existed in Redshift).
    To achieve this, it retrieves the necessary SQL syntax information from "drop_table_queries" 
    variable. Then, it uses cur.execute() to run the SQL syntax to drop tables.
    """
    for query in drop_table_queries:
        try:
            print("execute query: " + query)
            cur.execute(query)
            conn.commit()       
        except Exception as e:
            print(e)
        
def create_schema(cur, conn):
    """This function is for creating a schema in Redshift. To achieve this, it retrieves 
    the necessary SQL syntax information from "schema_create_query" variable. Then, it 
    uses cur.execute() to run the SQL syntax to create a schema.
    """
    try:
        print("execute query: " + schema_create_query)
        cur.execute(schema_create_query)
        conn.commit()           
    except Exception as e:
        print(e)
        
def create_tables(cur, conn):
    """This function is for creating tables in Redshift. It can be seperated into two parts.
    First part is creating staging tables in Redshift. To achieve this, it retrieves the 
    necessary SQL syntax information from "create_staging_table_queries" variable. Then, it
    uses cur.execute() to run the SQL syntax. Second part is about creating analytical tables
    in Redshift. It follows the same procedure as above.
    """
    # create staging tables
    for query in create_staging_table_queries:
        try:
            print("execute query: " + query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    
    # create analytical tables
    for query in create_analytical_table_queries:
        try:
            print("execute query: " + query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def main():
    """This function is for connecting, dropping table, and creating tables in Redshift. It 
    can be seperated into four parts. First part is using configparser package to read the 
    configuration file(dwh.cfg) and get the variable which is needed for connectting to 
    Redshift. Second part is about connectting to Redshift. Third part is relating to drop 
    and create tables in Redshift. This part need to call "drop_tables" and "create_tables" 
    function above. Fourth part is about closing the connection to Redshift.
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

    # create a schema and set path to this schema
    print("2. create a schema and set path to this schema")
    create_schema(cur, conn)
    cur.execute(schema_set_path_query)
    
    # drop tables and create tables
    print("3. Drop tables if tables are existed")
    drop_tables(cur, conn)    
    print("4. Create tables")
    create_tables(cur, conn)

    # close the connection
    print("5. Close connection") 
    conn.close()


if __name__ == "__main__":
    main()