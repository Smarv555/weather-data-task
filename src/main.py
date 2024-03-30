from weather_class import WeatherForecast


if __name__ == "__main__":
    # Extracting the weather forecast data and load it into a database
    weather_forecast_object = WeatherForecast()
    print(
        "--- Creating an object of class WeatherForecast that extracts weather forecast data from OpenWeatherMap API"
        "and saves it to a CSV file which is then loaded into a database for analyzes."
        f"\n------ Weather Forecast Table (metric units):"
        f"\n{weather_forecast_object.weather_data}"
        f"\n{weather_forecast_object.weather_data.dtypes}"
    )

    # Setting the hours forecast parameter to 24 hours
    hours_forecast = 24

    # Print how many distinct weather conditions were observed in a certain period.
    distinct_weather_conditions_df = weather_forecast_object.get_distinct_weather(hours_forecast=hours_forecast)
    print(
        "\n\n--- Print how many distinct weather conditions are observed in each city for the next 24 hours"
        f"\n------ Distinct weather conditions for 24 hours forecast:\n{distinct_weather_conditions_df}"
    )

    # Print the most common weather conditions in a certain period of time.
    most_common_weather_conditions_df = weather_forecast_object.get_most_common_weather(hours_forecast=hours_forecast)
    print(
        "\n\n--- Print the most common weather conditions in each city for the next 24 hours"
        f"\n------ Most common weather conditions for 24 hours forecast:\n{most_common_weather_conditions_df}"
    )

    # Print the average temperatures observed in a certain period per city.
    average_temperatures_df = weather_forecast_object.get_average_temp(hours_forecast=hours_forecast)
    print(
        "\n\n--- Print the average temperatures observed in each city for the next 24 hours"
        f"\n------ Average temperatures for 24 hours forecast:\n{average_temperatures_df}"
    )

    # Print the city with highest absolute temperature in a certain period of time.
    highest_temperature_city_df = weather_forecast_object.get_highest_temp_city(hours_forecast=hours_forecast)
    print(
        "\n\n--- Print the city with the highest temperature for the next 24 hours"
        f"\n------ Highest temperature city for 24 hours forecast:\n{highest_temperature_city_df}"
    )

    # Print the city with highest temperature variation in a certain period of time.
    highest_temperature_variation_city_df = weather_forecast_object.get_highest_temp_variation_city(hours_forecast=hours_forecast)
    print(
        "\n\n--- Print the city with the highest temperature variation for the next 24 hours"
        f"\n------ Highest temperature variation city for 24 hours forecast:\n{highest_temperature_variation_city_df}"
    )

    # Print the city with the strongest winds in a certain period of time.
    strongest_winds_city_df = weather_forecast_object.get_strongest_wind_city(hours_forecast=hours_forecast)
    print(
        "\n\n--- Print the city with the strongest winds for the next 24 hours"
        f"\n------ Strongest winds city for 24 hours forecast:\n{strongest_winds_city_df}"
    )
