import os
import configparser
import sys
import select
import psycopg2
import threading
import time
from confluent_kafka import Producer


def listen_to_postgres_channel():
    cur.execute(f"LISTEN {my_channel}")
    print(f"Start listen to PostgreSQL channel: {my_channel}")

    while True:

        # Checks if there is any data available to read from the conn socket (PostgreSQL).
        if select.select([conn], [], [], 5) == ([], [], []): 
            continue
        else:
            print("Get Notification")

            # Check if any PostgreSQL notifications have been sent
            conn.poll()

            # Loop iterates through any notifications that are pending
            while conn.notifies: 
                # Pops the first notification from the queue of pending notifications
                notify = conn.notifies.pop(0)
                notification_data = notify.payload.encode("utf-8")
                # Forward the notification to Kafka
                try:
                    producer.produce(kafka_topic, key='key', value=notification_data)
                    # To ensure the message is sent promptly
                    producer.flush()
                    print(f"Notification forwarded to Kafka topic: {notification_data}")
                except Exception as e:
                    print(f"Error forwarding the notification to Kafka topic: {e}")
        
        time.sleep(1)

if __name__ == '__main__':

    # Initialize the configuration parser
    config = configparser.ConfigParser()

    # Read configuration file
    config.read("./config.ini")

    try:
        kafka_topic = config.get('PROJ_CONF', 'KAFKA_TOPIC')
        kafka_broker = config.get('PROJ_CONF', 'KAFKA_BROKER')
        my_channel = config.get('PROJ_CONF', 'MY_CHANNEL')
    except Exception as e:
        print(f"Error cannot get required parameters: {e}")
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
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        print(f"Error cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # Create a cursor object and set autocommit to True
    try:
        cur = conn.cursor()
    except Exception as e:
        print(f"Error cannot create cursor object: {e}")
        sys.exit(1)
    
    # Initialize Kafka Producer
    try:
        producer = Producer({'bootstrap.servers': kafka_broker})
    except Exception as e:
        print(f"Error cannot create connection with Kafka producer: {e}")
        sys.exit(1)

    print(f"Kafka topic: {kafka_topic}")
    
    # Start listening to the PostgreSQL channel in a separate thread
    listener_thread = threading.Thread(target=listen_to_postgres_channel)
    listener_thread.start()
