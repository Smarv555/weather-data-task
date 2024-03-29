import os
import csv
import json
import requests
import logging
import responses
import pandas as pd
from datetime import datetime
from typing import List, Dict

# Logger setup
logger = logging.getLogger()
logging.basicConfig(
    filename="movies_dataset_logs.log", format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG
)


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
        raise Exception(f"Error: {response.status_code}. Failed to fetch data from API call {response.url}")
        # print(f"Error: {response.status_code}. Failed to fetch data.")
        # print("Response content:", response.content)


def convert_timestamp(timestamp: int) -> datetime:
    """

    :param timestamp:
    :return:
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


def generate_test_data(weather_data: str, test_data: str) -> None:
    """

    :param weather_data_csv:
    :param test_data_csv:
    :return:
    """
    weather_data_df = read_csv(file_path=weather_data)
    test_weather_data_df = weather_data_df.query("hours_forecast <= 5")
    test_weather_data_df.to_csv(path_or_buf=test_data, sep=",", header=True, index=False)


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

        response_list.append([geo_api_response[0], weather_api_response])

    with open(file, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


class ConfigParser:
    """
    A class for weather data configuration parser
    ...

    Attributes
    ----------
    config_json : dict
        config dictionary with movies dataset file paths

    Methods
    -------
    read_config_file():
        Read the config.json file
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

    # config = ConfigParser(env="main")
    #
    # api_key = config.api_key
    #
    # weather_api_call = config.weather_api
    #
    # geo_api_call = config.geocode_api
    #
    # loc_list = config.locations_list
    #
    # csv_file = config.weather_data_csv
    #
    # extract_weather_data_to_csv(
    #     file=csv_file,
    #     locations_list=loc_list,
    #     weather_api=weather_api_call,
    #     geo_api=geo_api_call,
    #     appid=api_key
    # )

    # @responses.activate
    # def responses_test():
    #     responses.get(
    #         "http://example.com/test?param1=param1&param2=param2.1%2Cparam2.2",
    #         json={"type": "get1"}
    #     )
    #
    #     responses.get(
    #         "http://example2.com/test?param1=param1&param2=param2.1%2Cparam2.2",
    #         json={"type": "get2"}
    #     )
    #
    #     response1 = get_data(
    #         api="http://example.com/test",
    #         payload={
    #             "param1": "param1",
    #             "param2": "param2.1,param2.2"
    #         }
    #     )
    #     print(response1)
    #
    #     response2 = get_data(
    #         api="http://example2.com/test",
    #         payload={
    #             "param1": "param1",
    #             "param2": "param2.1,param2.2"
    #         }
    #     )
    #     print(response2)
    #
    # responses_test()

    # weather_data = "../data/weather.csv"
    # test_data = "../data/test_weather.csv"
    # generate_test_data(weather_data=weather_data, test_data=test_data)

    print(convert_timestamp(1711612800))
    print(convert_timestamp(1711616400))
    print(convert_timestamp(1711620000))

    with open("../data/test_data/test_responses.json", "r") as f:
        test_responses = json.load(f)

    print(test_responses)