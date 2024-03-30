import json
import pytest
import responses
import pandas as pd
from pandas.testing import assert_frame_equal
from src.utils import ConfigParser, get_data, extract_weather_data_to_csv, read_csv


@responses.activate
def test_get_data():
    """
    Testing the get_data function for status code 200 and status code 400
    """
    # Mock API call response with status 200
    responses.get(
        url="http://example.com/test?param1=param1&param2=param2.1%2Cparam2.2",
        json={"type": "get1"},
        status=200
    )

    # Mock API call response with status 400
    responses.get(
        "http://example2.com/test?param1=param1&param2=param2.1%2Cparam2.2",
        status=400
    )

    # Calling the API with expected status 200
    response1 = get_data(
        api="http://example.com/test",
        payload={
            "param1": "param1",
            "param2": "param2.1,param2.2"
        }
    )

    # Calling the API with expected status 400
    with pytest.raises(Exception) as e:
        response2 = get_data(
            api="http://example2.com/test",
            payload={
                "param1": "param1",
                "param2": "param2.1,param2.2"
            }
        )

    expected_err_msg = "Error: 400. Failed to fetch data from API call " \
                       "http://example2.com/test?param1=param1&param2=param2.1%2Cparam2.2"

    assert response1 == {"type": "get1"}
    assert expected_err_msg == str(e.value)


@responses.activate
def test_extract_weather_data_to_csv():
    """
    Testing the extract_weather_data_to_csv function
    """
    config = ConfigParser(env="test")

    with open(config.test_responses_json, "r") as f:
        test_responses = json.load(f)

    # Geocode API Milan response
    responses.get(
        url=config.geocode_api + f"?q=Milan%2CIT&limit=1&appid={config.api_key}",
        json=test_responses["Milan"]["GeocodeAPI"]
    )
    # Weather API Milan response
    responses.get(
        url=config.weather_api + f"?lat=45.4641943&lon=9.1896346&exclude=daily%2Cminutely%2Ccurrent&units=metric&appid={config.api_key}",
        json=test_responses["Milan"]["WeatherAPI"]
    )
    # Geocode API Bologna response
    responses.get(
        url=config.geocode_api + f"?q=Bologna%2CIT&limit=1&appid={config.api_key}",
        json=test_responses["Bologna"]["GeocodeAPI"]
    )
    # Weather API Bologna response
    responses.get(
        url=config.weather_api + f"?lat=44.49381&lon=11.33875&exclude=daily%2Cminutely%2Ccurrent&units=metric&appid={config.api_key}",
        json=test_responses["Bologna"]["WeatherAPI"]
    )
    # Geocode API Cagliari response
    responses.get(
        url=config.geocode_api + f"?q=Cagliari%2CIT&limit=1&appid={config.api_key}",
        json=test_responses["Cagliari"]["GeocodeAPI"]
    )
    # Weather API Cagliari response
    responses.get(
        url=config.weather_api + f"?lat=39.227779&lon=9.111111&exclude=daily%2Cminutely%2Ccurrent&units=metric&appid={config.api_key}",
        json=test_responses["Cagliari"]["WeatherAPI"]
    )
    # Calling the extract weather data function and saving the test weather data to CSV
    extract_weather_data_to_csv(
        file=config.weather_data_csv,
        locations_list=config.locations_list,
        weather_api=config.weather_api,
        geo_api=config.geocode_api,
        appid=config.api_key
    )
    # Creating pandas data frame from the test weather data
    actual_df = read_csv(file_path=config.weather_data_csv)

    # Creating pandas data frame for the expected weather data
    expected_df = pd.DataFrame(
        data={
            "hours_forecast": [1, 2, 3, 1, 2, 3, 1, 2, 3],
            "datetime": [
                "2024-03-28 10:00:00",
                "2024-03-28 11:00:00",
                "2024-03-28 12:00:00",
                "2024-03-28 10:00:00",
                "2024-03-28 11:00:00",
                "2024-03-28 12:00:00",
                "2024-03-28 10:00:00",
                "2024-03-28 11:00:00",
                "2024-03-28 12:00:00"
            ],
            "country": ["IT", "IT", "IT", "IT", "IT", "IT", "IT", "IT", "IT"],
            "city": ["Milan", "Milan", "Milan", "Bologna", "Bologna", "Bologna", "Cagliari", "Cagliari", "Cagliari"],
            "temp": [7.36, 7.36, 7.43, 8.36, 7.36, 7.43, 10.36, 7.36, 7.43],
            "temp_feels_like": [5.48, 5.77, 6.78, 5.48, 5.77, 6.78, 8.48, 5.77, 6.78],
            "weather": [
                "Rain",
                "Clouds",
                "Clouds",
                "Rain",
                "Rain",
                "Rain",
                "Clouds",
                "Clouds",
                "Clouds"
            ],
            "weather_description": [
                "light rain",
                "overcast clouds",
                "overcast clouds",
                "light rain",
                "light rain",
                "light rain",
                "overcast clouds",
                "overcast clouds",
                "overcast clouds"
            ],
            "pop": [1, 0.8, 0.8, 1, 0.8, 0.8, 1, 0.8, 0.8],
            "wind_speed_m_s": [2.79, 2.41, 1.45, 3.79, 2.41, 1.45, 3.79, 2.41, 1.45],
            "clouds_percentage": [100, 100, 100, 100, 100, 100, 100, 100, 100],
            "pressure_level": [997, 997, 998, 997, 997, 998, 997, 997, 998],
            "humidity_percentage": [94, 94, 93, 94, 94, 93, 94, 94, 93]
        },
    )

    # Asserting the expected and actual pandas data frames
    assert_frame_equal(expected_df, actual_df)



