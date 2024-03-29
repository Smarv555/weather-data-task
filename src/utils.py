import os
import csv
import json
import requests
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict

# Logger setup
logger = logging.getLogger()
logging.basicConfig(
    filename="weather_forecast_logs.log", format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG
)


def get_data(api: str, payload: dict) -> dict:
    """
    Fetch data from API call
    :param api: API call url
    :param payload: payload parameters for the API call
    :return: A dictionary with the API call response
    """
    response = requests.get(api, params=payload)
    if response.status_code == 200:
        logger.info(f"Successfully fetched the data from API call: {response.url}")
        return response.json()
    else:
        error_msg = f"Error: {response.status_code}. Failed to fetch data from API call {response.url}"
        logger.error(error_msg)
        raise Exception(error_msg)


def convert_timestamp(timestamp: int) -> datetime:
    """
    Converts a timestamp value to datetime
    :param timestamp: timestamp value to be converted
    :return: converted datetime
    """
    return datetime.fromtimestamp(timestamp)


def read_csv(file_path: str) -> pd.DataFrame:
    """
    Read csv file and return a pandas data frame
    :param file_path: Input file path
    :return: Pandas data frame
    """
    function_name = read_csv.__name__
    try:
        logger.info(
            f"Calling function {function_name} on file {file_path}."
        )
        df = pd.read_csv(filepath_or_buffer=file_path, header=0, low_memory=False)
    except Exception as error:
        error_msg = f"Error occurred in {function_name} function: {error}"
        logger.error(error_msg)
        raise Exception(error_msg)

    logger.info(
        f"The {function_name} function finished successfully. Data frame created from CSV file: {file_path}"
    )
    return df


def extract_weather_data_to_csv(
        file: str,
        locations_list: List[Dict],
        weather_api: str,
        geo_api: str,
        appid: str) -> None:
    """
    Extract weather forecast data from the OpenWeatherMap API and save it to CSV file
    :param file: CSV file path
    :param locations_list: list of country codes and cities which weather data will be extracted
    :param weather_api: weather forecast OpenWeatherMap API url
    :param geo_api: geocoding OpenWeatherMap API url
    :param appid: OpenWeatherMap API key
    """
    function_name = extract_weather_data_to_csv.__name__
    logger.info(f"Calling function {function_name} on file {file}")
    fieldnames = [
        "hours_forecast",
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

    try:
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

            # Integer for hours_forecast column
            hours_forecast = 1

            for hour_dict in weather_api_response.get("hourly"):
                data.append(
                    {
                        "hours_forecast": hours_forecast,
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
                hours_forecast += 1

        with open(file, "w", encoding="UTF8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.info(f"The {function_name} function finished successfully. Weather data extracted to CSV file: {file}")
    except Exception as error:
        error_msg = f"Error occurred in {extract_weather_data_to_csv.__name__} function: {error}"
        logger.error(error_msg)
        raise Exception(error_msg)


class ConfigParser:
    """
    A class for weather forecast configuration parser
    ...

    Attributes
    ----------
    config_json : dict
        config dictionary for weather forecast data
    api_key : str
        OpenWeatherMap API key
    weather_api : str
        weather forecast OpenWeatherMap API url
    geocode_api : str
        geocoding OpenWeatherMap API url
    locations_list : List[Dict]
        list of country codes and cities which weather data will be extracted
    database : str
        SQL database engine url
    table_name : str
        SQL database table name
    weather_data_csv : str
        weather data CSV file path
    test_responses_json : str
        JSON file path for the unit test response mocking
    Methods
    -------
    read_config_file:
        Read the config-{env}.json file
    """

    def __init__(self, env):
        # Read config file
        self.config_json = self.read_config_file(env)
        # Set config parser attributes
        self.api_key = self.config_json.get("api_key")
        self.weather_api = self.config_json.get("weather_api")
        self.geocode_api = self.config_json.get("geocode_api")
        self.locations_list = self.config_json.get("locations_list")
        self.database = self.config_json.get("database")
        self.table_name = self.config_json.get("table_name")
        self.weather_data_csv = os.path.abspath(self.config_json.get("weather_data_csv"))
        self.test_responses_json = os.path.abspath(self.config_json.get("test_responses_json"))

    def read_config_file(self, env) -> dict:
        """
        Read the config.json file
        :param env: A string indicating which config file to be used. It could be either 'main' or 'test'.
        :return: JSON file with configuration
        """
        try:
            config_file_path = os.path.join(
                os.path.dirname(__file__),
                "..",
                "config",
                f"config-{env}.json"
            )
            with open(config_file_path, "r", encoding="utf-8") as config_file:
                config_json = json.load(config_file)
        except Exception as error:
            error_msg = f"Error occurred in {self.read_config_file.__name__} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return config_json
