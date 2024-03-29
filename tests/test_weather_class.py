import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from src.weather_class import WeatherForecast


test_weather_forecast_object = WeatherForecast(test=True)
hours_forecast = 3


def test_get_distinct_weather():
    """
    Testing the get_distinct_weather method from WeatherForecast class
    """
    expected_data = {
        "city": ["Bologna", "Cagliari", "Milan", "Milan"],
        "weather": ["Rain", "Clouds", "Clouds", "Rain"],
        "weather_description": ["light rain", "overcast clouds", "overcast clouds", "light rain"],
        "percentage": [100.0, 100.0, 67.0, 33.0]
    }

    expected_df = pd.DataFrame(data=expected_data)
    actual_df = test_weather_forecast_object.get_distinct_weather(hours_forecast=hours_forecast)

    assert_frame_equal(expected_df, actual_df)


def test_get_most_common_weather():
    """
    Testing the get_most_common_weather method from WeatherForecast class
    """
    expected_data = {
        "city": ["Bologna", "Cagliari", "Milan"],
        "weather": ["Rain", "Clouds", "Clouds"],
        "weather_description": ["light rain", "overcast clouds", "overcast clouds"]
    }

    expected_df = pd.DataFrame(data=expected_data)
    actual_df = test_weather_forecast_object.get_most_common_weather(hours_forecast=hours_forecast)

    assert_frame_equal(expected_df, actual_df)


def test_get_average_temp():
    """
    Testing the get_average_temp method from WeatherForecast class
    """
    expected_data = {
        "city": ["Bologna", "Cagliari", "Milan"],
        "average_temp": [7.72, 8.38, 7.38]
    }

    expected_df = pd.DataFrame(data=expected_data)
    actual_df = test_weather_forecast_object.get_average_temp(hours_forecast=hours_forecast)

    assert_frame_equal(expected_df, actual_df)


def test_get_highest_temp_city():
    """
    Testing the get_highest_temp_city method from WeatherForecast class
    """
    expected_data = {
        "datetime": ["2024-03-28 10:00:00"],
        "city": ["Cagliari"],
        "highest_temp": [10.36]
    }

    expected_df = pd.DataFrame(data=expected_data)
    actual_df = test_weather_forecast_object.get_highest_temp_city(hours_forecast=hours_forecast)

    assert_frame_equal(expected_df, actual_df)


def test_get_highest_temp_variation_city():
    """
    Testing the get_highest_temp_variation_city method from WeatherForecast class
    """
    expected_data = {
        "city": ["Cagliari"],
        "highest_temp_variation": [3.0]
    }

    expected_df = pd.DataFrame(data=expected_data)
    actual_df = test_weather_forecast_object.get_highest_temp_variation_city(hours_forecast=hours_forecast)

    assert_frame_equal(expected_df, actual_df)


def test_get_strongest_wind_city():
    """
    Testing the get_strongest_wind_city method from WeatherForecast class
    """
    expected_data = {
        "datetime": ["2024-03-28 10:00:00", "2024-03-28 10:00:00"],
        "city": ["Bologna", "Cagliari"],
        "wind_speed_m_s": [3.79, 3.79]
    }

    expected_df = pd.DataFrame(data=expected_data)
    actual_df = test_weather_forecast_object.get_strongest_wind_city(hours_forecast=hours_forecast)

    assert_frame_equal(expected_df, actual_df)
