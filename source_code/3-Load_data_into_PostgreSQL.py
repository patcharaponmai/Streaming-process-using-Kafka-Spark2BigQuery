import os
import sys
import time
import pandas as pd
import psycopg2
import configparser

def interact_postgres_db():

    """
        This function perform create target table and trigger for notice data change in PostgreSQL 
    """

    print("================================================")
    print("===== Create Table & Trigger in PostgreSQL =====")
    print("================================================")
    print()

    # Drop Trigger
    try:
        cur.execute(f"DROP TRIGGER IF EXISTS {my_trigger} on {TARGET_TABLE};")
        print(f"Drop trigger {my_trigger} success.")
    except Exception as e:
        print(f"Error cannot drop trigger: {e}")
        sys.exit(1)

    # Drop Table
    try:
        cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        print(f"Drop {TARGET_TABLE} success.")
    except Exception as e:
        print(f"Error cannot drop table {TARGET_TABLE}: {e}")
        sys.exit(1)

    # Create target table in PostgreSQL
    CREATE_TABLE_SQL = f""" CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                        "event_id" VARCHAR,
                        "name" VARCHAR,
                        "event_name" VARCHAR,
                        "category" VARCHAR,
                        "item_id" VARCHAR,
                        "item_quantity" INTEGER,
                        "event_time" TIMESTAMP)
    """

    try:
        cur.execute(CREATE_TABLE_SQL)
        print(f"Create table if not exists {TARGET_TABLE} success.")
    except Exception as e:
        print(f"Error cannot create table {TARGET_TABLE}: {e}")
        sys.exit(1)

    # Read the SQL file
    SQL_STATEMENT = f"""
        -- PostgreSQL notification function to send notify to my channel
        CREATE OR REPLACE FUNCTION notify_data_change() RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('{my_channel}', row_to_json(NEW)::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        -- PostgreSQL trigger to activate the notification function
        CREATE TRIGGER {my_trigger}
        AFTER INSERT ON {TARGET_TABLE}
        FOR EACH ROW
        EXECUTE FUNCTION notify_data_change();
    """
    
    # Create a PostgreSQL notification for detect change in table
    try:
        cur.execute(SQL_STATEMENT)
        print(f"Create trigger for detect change in table success.")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Trigger '{my_trigger}' already exists for table '{TARGET_TABLE}'.")
        else:
            print(f"Error creating trigger: {e}")
            sys.exit(1)

    # Enable trigger to target table for notice change
    ENABLE_TRIGGER_SQL = f"ALTER TABLE {TARGET_TABLE} ENABLE TRIGGER {my_trigger};"
    try:
        cur.execute(ENABLE_TRIGGER_SQL)
        print(f"Enable trigger '{my_trigger}' success.\n")
    except Exception as e:
        print(f"Error enable trigger '{my_trigger}': {e}")
        sys.exit(1)

    print()
    print("================================================")
    print("==== Complete Table & Trigger in PostgreSQL ====")
    print("================================================")
    print()

def main():

    interact_postgres_db()

    while True:
        check_status = input("Is streaming process available (y/n)?: ")
        if str.lower(check_status) == 'y':
            break

    print("================================================")
    print("========== Loading Data to PostgreSQL ==========")
    print("================================================")
    print()

    df = pd.read_csv("./output.csv")

    column_list = df.columns
    column_str = ",".join(column_list)
    TOTAL_REC = len(df)

    print(f"======== START INGEST DATA INTO `{db_name}`.`{TARGET_TABLE}` ========")

    for index, row in df.iterrows():
        values = []

        for column_name in column_list:
            values.append(row[column_name])

        # The data contains various data types, and these placeholders will be used in a join operation to create a set of columns.
        placeholders = ", ".join(["%s"] * len(values))
        value = tuple(values)

        # Create query insert statement
        INSERT_CMD = f"""INSERT INTO {TARGET_TABLE} ({column_str}) VALUES ({placeholders})\n"""

        # Create query insert statement
        try:
            cur.execute(INSERT_CMD, value)
        except Exception as e:
            print(f"Error cannot insert data into table {TARGET_TABLE} : {e}")
            sys.exit(1)

        message : str = f"ROWS INSERT STATUS ------ [{index}/{TOTAL_REC}] ------"
        print(message)

        time.sleep(2)

    print()
    print("================================================")
    print("============ Complete Loading Data =============")
    print("================================================")

######################################
############ MAIN PROGRAM ############
######################################

if __name__ == '__main__':

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        TARGET_TABLE = config.get('PROJ_CONF', 'TABLE_NAME')
        my_channel = config.get('PROJ_CONF', 'MY_CHANNEL')
        my_trigger = config.get('PROJ_CONF', 'MY_TRIGGER')
    except Exception as e:
        print(f"Error cannot get require parameters: {e}")
        sys.exit(1)

    db_name = os.getenv('PGDATABASE')
    db_user = os.getenv('PGUSER')
    db_password = os.getenv('PGPASSWORD')
    db_host = os.getenv('PGHOST')

    db_params = {
        "database": db_name,
        "user": db_user,
        "password": db_password,
        "host": db_host
    }

    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_session(autocommit=True)
    except Exception as e:
        print(f"Error cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # Create a cursor object and set autocommit to True
    try:
        cur = conn.cursor()
    except Exception as e:
        print(f"Error cannot craete cursor object: {e}")
        sys.exit(1)

    main()