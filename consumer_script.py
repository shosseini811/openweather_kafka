from confluent_kafka import Consumer, KafkaError
import json

def main():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest',
        'receive.message.max.bytes': 1e25  # increase this value as needed

    })

    c.subscribe(['weather_data'])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        weather_data = json.loads(msg.value().decode('utf-8'))
        print(weather_data)

    c.close()

if __name__ == "__main__":
    main()
