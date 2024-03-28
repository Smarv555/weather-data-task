import csv
import json
import requests
import pandas as pd
from datetime import datetime

api_key = "fc19154afa75c5adf7ac46e224283180"
lat = 45.4641943
lon = 9.1896346

params = {
    "lat": lat,
    "lon": lon,
    "exclude": "daily,minutely,current",
    "units": "metric",
    "appid": api_key,
}

api_call = "https://api.openweathermap.org/data/3.0/onecall"

geo_api_call = "http://api.openweathermap.org/geo/1.0/direct"

geo_params = {
    "q": "Milano",
    "limit": 1,
    "appid": api_key,
}


def get_data(api: str, payload: dict) -> dict:
    """

    :param api:
    :param payload:
    :return:
    """
    response = requests.get(api, params=payload)
    if response.status_code == 200:
        print("Successfully fetched the data")
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


def save_weather_data_to_csv(file: str, weather_dict: dict, locations_list: list) -> None:
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


if __name__ == "__main__":
    # response = get_data(api=api_call, payload=params)
    # with open("weather_data.json", "w") as f:
    #     json.dump(response, f)

    # dt = 1711609200
    # dt_converted = datetime.fromtimestamp(dt)
    #
    # print(dt, type(dt_converted))

    with open("weather_data.json", "r") as f:
        weather_dic = json.load(f)

    loc_list = [
        {
            "country": "IT",
            "city": "Milan"
        }
    ]

    csv_file = "data/weather.csv"

    save_weather_data_to_csv(file=csv_file, weather_dict=weather_dic, locations_list=loc_list)

