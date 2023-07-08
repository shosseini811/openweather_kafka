from confluent_kafka import Consumer, KafkaError
import json

def is_severe_weather(weather_data):
    # implement your own logic here
    return False

def send_alert(city):
    print(f"Alert: severe weather in {city}")

def main():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'weather_analysis',
        'auto.offset.reset': 'earliest'
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

        if is_severe_weather(weather_data):
            send_alert(weather_data['name'])

    c.close()

if __name__ == "__main__":
    main()
