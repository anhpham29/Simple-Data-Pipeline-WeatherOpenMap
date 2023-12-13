# src/etl/elt_weather.py

import json
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlencode
import requests

def get_api_key():
    try:
        with open('src/config/api_private_key.json', 'r') as key_file:
            keys = json.load(key_file)
            return keys.get('openweathermap_api_key', None)
    except FileNotFoundError:
        print("Error: api_private_key.json file not found.")
        return None

def fetch_weather_data():
    api_key = get_api_key()
    if api_key:
        try:
            # Load list of cities from JSON file
            with open('data/processed/cities.json', 'r') as json_file:
                cities_data = json.load(json_file)

            base_url = 'http://api.openweathermap.org/data/2.5/weather'

            # Create an empty list to store weather data
            weather_data_list = []

            # Fetch weather data for each city
            for city_data in cities_data:
                city = city_data['city']
                country_code = city_data['country_code']

                params = {
                    'q': f'{city},{country_code}',
                    'appid': api_key
                }

                # Construct the API endpoint using urljoin
                api_endpoint = urljoin(base_url, '?' + urlencode(params))

                response = requests.get(api_endpoint)
                response.raise_for_status()
                weather_data = response.json()
                temperature = weather_data['main']['temp']
                description = weather_data['weather'][0]['description']
                timestamp = weather_data['dt']
                date_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

                # Append new data to the list
                weather_data_list.append({
                    'City': city,
                    'CountryCode': country_code,
                    'DateTime': date_time,
                    'Temperature': temperature,
                    'Description': description
                })

            # Create a DataFrame from the list
            df = pd.DataFrame(weather_data_list)

            # Export weather data to CSV
            df.to_csv('data/processed/weather_data.csv', index=False)
            print('Weather data exported to data/processed/weather_data.csv')

        except FileNotFoundError:
            print('Error: data/processed/cities.json file not found.')
    else:
        print("Error: API key is not available.")

if __name__ == "__main__":
    fetch_weather_data()
