import csv
import json
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict


def get_data(api: str, payload: dict) -> dict:
    """

    :param api:
    :param payload:
    :return:
    """
    response = requests.get(api, params=payload)
    if response.status_code == 200:
        print(f"Successfully fetched the data from API call: {response.url}")
        return response.json()
    else:
        print(f"Error: {response.status_code}. Failed to fetch data.")
        print("Response content:", response.content)


def convert_timestamp(timestamp: int) -> datetime:
    """

    :param timestamp:
    :return:
    """
    return datetime.fromtimestamp(timestamp)


def save_weather_data_to_csv_old(file: str, weather_dict: dict, locations_list: list) -> None:
    """

    :param file:
    :param weather_dict:
    :param locations_list:
    :return:
    """
    fieldnames = [
        "time_id",
        "datetime",
        "country",
        "city",
        "temp",
        "temp_feels_like",
        "weather",
        "weather_description",
        "pop",
        "wind_speed_m_s",
        "clouds_percentage",
        "pressure_level",
        "humidity_percentage",
    ]
    data = []

    for location in locations_list:
        country = location.get("country")
        city = location.get("city")
        time_id = 1
        for hour_dict in weather_dict.get("hourly"):
            data.append(
                {
                    "time_id": time_id,
                    "datetime": convert_timestamp(hour_dict.get("dt")),
                    "country": country,
                    "city": city,
                    "temp": hour_dict.get("temp"),
                    "temp_feels_like": hour_dict.get("feels_like"),
                    "weather": hour_dict.get("weather")[0].get("main"),
                    "weather_description": hour_dict.get("weather")[0].get("description"),
                    "pop": hour_dict.get("pop"),
                    "wind_speed_m_s": hour_dict.get("wind_speed"),
                    "clouds_percentage": hour_dict.get("clouds"),
                    "pressure_level": hour_dict.get("pressure"),
                    "humidity_percentage": hour_dict.get("humidity"),
                }
            )
            time_id += 1

    with open(file, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def extract_weather_data_to_csv(
        file: str,
        locations_list: List[Dict],
        weather_api: str,
        geo_api: str,
        appid: str) -> None:
    """

    :param file:
    :param locations_list:
    :param weather_api:
    :param geo_api:
    :param appid:
    """
    fieldnames = [
        "time_id",
        "datetime",
        "country",
        "city",
        "temp",
        "temp_feels_like",
        "weather",
        "weather_description",
        "pop",
        "wind_speed_m_s",
        "clouds_percentage",
        "pressure_level",
        "humidity_percentage",
    ]
    response_list = []
    data = []

    for location in locations_list:
        # Geocoding API
        city = location.get("city")
        country = location.get("country")
        geo_api_payload = {
            "q": f"{city},{country}",
            "limit": 1,
            "appid": appid
        }
        geo_api_response = get_data(api=geo_api, payload=geo_api_payload)

        # Weather forecast API
        weather_api_payload = {
            "lat": geo_api_response[0].get("lat"),
            "lon": geo_api_response[0].get("lon"),
            "exclude": "daily,minutely,current",
            "units": "metric",
            "appid": appid,
        }
        weather_api_response = get_data(api=weather_api, payload=weather_api_payload)

        # Integer for time_id column
        time_id = 1

        for hour_dict in weather_api_response.get("hourly"):
            data.append(
                {
                    "time_id": time_id,
                    "datetime": convert_timestamp(hour_dict.get("dt")),
                    "country": country,
                    "city": city,
                    "temp": hour_dict.get("temp"),
                    "temp_feels_like": hour_dict.get("feels_like"),
                    "weather": hour_dict.get("weather")[0].get("main"),
                    "weather_description": hour_dict.get("weather")[0].get("description"),
                    "pop": hour_dict.get("pop"),
                    "wind_speed_m_s": hour_dict.get("wind_speed"),
                    "clouds_percentage": hour_dict.get("clouds"),
                    "pressure_level": hour_dict.get("pressure"),
                    "humidity_percentage": hour_dict.get("humidity"),
                }
            )
            time_id += 1

        response_list.append([geo_api_response[0], weather_api_response])

    with open(file, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    # response = get_data(api=api_call, payload=params)
    # with open("weather_data.json", "w") as f:
    #     json.dump(response, f)

    # dt = 1711609200
    # dt_converted = datetime.fromtimestamp(dt)
    #
    # print(dt, type(dt_converted))

    # with open("../weather_data.json", "r") as f:
    #     weather_dic = json.load(f)

    api_key = "fc19154afa75c5adf7ac46e224283180"

    weather_api_call = "https://api.openweathermap.org/data/3.0/onecall"

    geo_api_call = "http://api.openweathermap.org/geo/1.0/direct"

    loc_list = [
        {
            "country": "IT",
            "city": "Milano"
        },
        {
            "country": "IT",
            "city": "Bologna"
        },
        {
            "country": "IT",
            "city": "Cagliari"
        },
    ]

    csv_file = "../data/weather.csv"

    extract_weather_data_to_csv(
        file=csv_file,
        locations_list=loc_list,
        weather_api=weather_api_call,
        geo_api=geo_api_call,
        appid=api_key
    )

