import pandas as pd
from sqlalchemy import create_engine
from utils import logger, read_csv, extract_weather_data_to_csv, ConfigParser


class WeatherForecast:
    """
    A class for weather forecast data
    ...

    Attributes
    ----------
    config : dict
        config dictionary for weather forecast data
    engine : str
        SQL database engine
    weather_data: pd.Dataframe
        Pandas data frame with weather forecast data
    max_hours_forecast: int
        the maximum hour forecast value
    Methods
    -------
    get_distinct_weather:
        Get all distinct weather conditions in a certain period of time per city
    get_most_common_weather:
        Get the most common weather conditions in a certain period of time per city
    get_average_temp:
        Get the average temperature in a certain period of time per city
    get_highest_temp_city:
        Get the city with the highest absolute temperature in a certain period of time
    get_highest_temp_variation_city:
        Get the city with the highest daily temperature variation in a certain period of time
    get_strongest_wind_city:
        Get the city with the strongest wind in a certain period of time
    check_hours_forecast:
        Check if the hours forecasting period is in the accepted range, [1, {self.max_hours_forecast}]
    """

    def __init__(self, test: bool = False):
        # Set config attribute for the file paths configuration for main or test env
        if not test:
            self.config = ConfigParser(env="main")
            # Extract the weather data from the OpenWeatherMap API
            extract_weather_data_to_csv(
                file=self.config.weather_data_csv,
                locations_list=self.config.locations_list,
                weather_api=self.config.weather_api,
                geo_api=self.config.geocode_api,
                appid=self.config.api_key
            )
        else:
            self.config = ConfigParser(env="test")

        # Create the db engine
        self.engine = create_engine(url=self.config.database)
        # Create data frame from the weather csv
        self.weather_data = read_csv(self.config.weather_data_csv)
        # Get the max hours forecast value
        self.max_hours_forecast = self.weather_data["hours_forecast"].max()
        # Store the weather data frame as table (if table exists it is dropped and replaced)
        self.weather_data.to_sql(name=self.config.table_name, con=self.engine, if_exists="replace")

    def get_distinct_weather(self, hours_forecast: int = None) -> pd.DataFrame:
        """
        Get all distinct weather conditions in a certain period of time per city
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return: Pandas data frame with distinct weather conditions per city
        """
        hours_forecast = self.max_hours_forecast if not hours_forecast else hours_forecast
        self.check_hours_forecast(hours_forecast=hours_forecast)
        method_name = self.get_distinct_weather.__name__
        query = f"""
            SELECT DISTINCT
                city,
                weather,
                weather_description,
                ROUND(COUNT(weather)/{float(hours_forecast)}*100, 0) AS percentage
            FROM weather_table
            WHERE hours_forecast <= {hours_forecast}
            GROUP BY city, weather, weather_description
            ORDER BY city, percentage DESC;
        """
        try:
            logger.info(
                f"Calling {self.__class__.__name__} method {method_name} with query: {query}"
            )
            df_query_result = pd.read_sql_query(sql=query, con=self.engine)
        except Exception as error:
            error_msg = f"Error occurred in {method_name} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return df_query_result

    def get_most_common_weather(self, hours_forecast: int = None) -> pd.DataFrame:
        """
        Get the most common weather conditions in a certain period of time per city
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return: Pandas data frame with most common weather conditions per city
        """
        hours_forecast = self.max_hours_forecast if not hours_forecast else hours_forecast
        self.check_hours_forecast(hours_forecast=hours_forecast)
        method_name = self.get_most_common_weather.__name__
        query = f"""
            SELECT
                city,
                weather,
                weather_description
            FROM (
                SELECT DISTINCT
                    city,
                    weather,
                    weather_description,
                ROUND(COUNT(weather)/{float(hours_forecast)}*100, 0) AS percentage
                FROM weather_table
                WHERE hours_forecast <= {hours_forecast}
                GROUP BY city, weather, weather_description
            )
            GROUP BY city
            HAVING percentage == MAX(percentage);
        """
        try:
            logger.info(
                f"Calling {self.__class__.__name__} method {method_name} with query: {query}"
            )
            df_query_result = pd.read_sql_query(sql=query, con=self.engine)
        except Exception as error:
            error_msg = f"Error occurred in {method_name} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return df_query_result

    def get_average_temp(self, hours_forecast: int = None) -> pd.DataFrame:
        """
        Get the average temperature in a certain period of time per city
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return: Pandas data frame with average temperature per city
        """
        hours_forecast = self.max_hours_forecast if not hours_forecast else hours_forecast
        self.check_hours_forecast(hours_forecast=hours_forecast)
        method_name = self.get_average_temp.__name__
        query = f"""
            SELECT
                city,
                ROUND(AVG(temp), 2) AS average_temp
            FROM weather_table
            WHERE hours_forecast <= {hours_forecast}
            GROUP BY city;
        """
        try:
            logger.info(
                f"Calling {self.__class__.__name__} method {method_name} with query: {query}"
            )
            df_query_result = pd.read_sql_query(sql=query, con=self.engine)
        except Exception as error:
            error_msg = f"Error occurred in {method_name} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return df_query_result

    def get_highest_temp_city(self, hours_forecast: int = None) -> pd.DataFrame:
        """
        Get the city with highest absolute temperature in a certain period of time
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return: Pandas data frame with highest temperature city
        """
        hours_forecast = self.max_hours_forecast if not hours_forecast else hours_forecast
        self.check_hours_forecast(hours_forecast=hours_forecast)
        method_name = self.get_highest_temp_city.__name__
        query = f"""
            SELECT
                datetime,
                city,
                temp AS highest_temp
            FROM weather_table
            WHERE hours_forecast <= {hours_forecast}
            AND temp = (SELECT MAX(temp) FROM weather_table WHERE hours_forecast <= {hours_forecast});
        """
        try:
            logger.info(
                f"Calling {self.__class__.__name__} method {method_name} with query: {query}"
            )
            df_query_result = pd.read_sql_query(sql=query, con=self.engine)
        except Exception as error:
            error_msg = f"Error occurred in {method_name} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return df_query_result

    def get_highest_temp_variation_city(self, hours_forecast: int = None) -> pd.DataFrame:
        """
        Get the city with the highest daily temperature variation in a certain period of time
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return: Pandas data frame with the highest temperature variation city
        """
        hours_forecast = self.max_hours_forecast if not hours_forecast else hours_forecast
        self.check_hours_forecast(hours_forecast=hours_forecast)
        method_name = self.get_highest_temp_variation_city.__name__
        query = f"""
            SELECT
                city,
                MAX(temp_variation) AS highest_temp_variation,
                max_temp,
                min_temp
            FROM (
                SELECT
                    city,
                    MAX(temp) as max_temp,
                    MIN(temp) as min_temp,
                    (MAX(temp) - MIN(temp)) AS temp_variation
                FROM weather_table
                WHERE hours_forecast <= {hours_forecast}
                GROUP BY city
            );
        """
        try:
            logger.info(
                f"Calling {self.__class__.__name__} method {method_name} with query: {query}"
            )
            df_query_result = pd.read_sql_query(sql=query, con=self.engine)
        except Exception as error:
            error_msg = f"Error occurred in {method_name} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return df_query_result

    def get_strongest_wind_city(self, hours_forecast: int = None) -> pd.DataFrame:
        """
        Get the city with the strongest wind in a certain period of time
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return: Pandas data frame with the strongest wind city
        """
        hours_forecast = self.max_hours_forecast if not hours_forecast else hours_forecast
        self.check_hours_forecast(hours_forecast=hours_forecast)
        method_name = self.get_strongest_wind_city.__name__
        query = f"""
            SELECT
                datetime,
                city,
                wind_speed_m_s
            FROM weather_table
            WHERE hours_forecast <= {hours_forecast}
            AND wind_speed_m_s = (SELECT MAX(wind_speed_m_s) FROM weather_table WHERE hours_forecast <= {hours_forecast});
        """
        try:
            logger.info(
                f"Calling {self.__class__.__name__} method {method_name} with query: {query}"
            )
            df_query_result = pd.read_sql_query(sql=query, con=self.engine)
        except Exception as error:
            error_msg = f"Error occurred in {method_name} method: {error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        return df_query_result

    def check_hours_forecast(self, hours_forecast: int) -> None:
        """
        Check if the hours forecasting period is in the accepted range, [1, {self.max_hours_forecast}]
        :param hours_forecast: forecasting period in hours, should be in range [1, {self.max_hours_forecast}]
        :return:
        """
        hours_in_range = hours_forecast in range(1, self.max_hours_forecast + 1)
        if not hours_in_range:
            error_msg = f"The {self.__class__.__name__} method {self.check_hours_forecast.__name__} failed. " \
                        f"Invalid hours forecast, please specify hours in range [1, {self.max_hours_forecast}]"
            logger.error(error_msg)
            raise ValueError(error_msg)


if __name__ == "__main__":
    weather_object = WeatherForecast()
    hours = 24
    print(f"Print distinct weather conditions per city:"
          f"\n{weather_object.get_distinct_weather(hours_forecast=hours)}")
    print(f"Print most common weather conditions per city:"
          f"\n{weather_object.get_most_common_weather(hours_forecast=hours)}")
    print(f"Print average temperature per city:"
          f"\n{weather_object.get_average_temp(hours_forecast=hours)}")
    print(f"Print highest temp city:"
          f"\n{weather_object.get_highest_temp_city(hours_forecast=hours)}")
    print(f"Print highest temp variation city:"
          f"\n{weather_object.get_highest_temp_variation_city(hours_forecast=hours)}")
    print(f"Print strongest wind city:"
          f"\n{weather_object.get_strongest_wind_city(hours_forecast=hours)}")
