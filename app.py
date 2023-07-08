import requests
import json
import time
from kafka import KafkaProducer
from config import api_key

def get_weather(api_key, city):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'imperial'  # For temperature in Celsius use 'metric'
    }
    response = requests.get(base_url, params=params)
    weather_data = response.json()
    return weather_data

def main():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Update with your Kafka broker IPs
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    city = 'Cleveland,OH,USA'
    while True:
        weather_data = get_weather(api_key, city)
        print(f"Current temperature in {city} is {weather_data['main']['temp']}°F")
        
        # Send the weather data to Kafka
        producer.send('weather_data', weather_data)
        producer.flush()
        
        time.sleep(1)  # wait for 60 seconds before making the next request

if __name__ == "__main__":
    main()
