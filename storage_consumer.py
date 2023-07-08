import sqlite3
from confluent_kafka import Consumer, KafkaError
import json

def main():
    conn = sqlite3.connect('weather.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            city TEXT,
            weather TEXT,
            temperature REAL,
            timestamp INTEGER
        )
    ''')

    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'weather_group',
        'auto.offset.reset': 'earliest',
        'receive.message.max.bytes': 2147483647  # increase this value as needed

    }

    consumer = Consumer(conf)

    consumer.subscribe(['weather_data'])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        message = json.loads(msg.value().decode('utf-8'))
        city = message['name']
        weather = message['weather'][0]['description']
        temp = message['main']['temp']
        timestamp = message['dt']

        c.execute('''
            INSERT INTO weather_data (city, weather, temperature, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (city, weather, temp, timestamp))
        conn.commit()

    consumer.close()

if __name__ == "__main__":
    main()
