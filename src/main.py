import pandas as pd
from sqlalchemy import create_engine

# pd.set_option("display.max_columns", 15)

data = pd.read_csv("../data/main_data/weather.csv")
max_hour_forecast = data["hours_forecast"].max()
print(max_hour_forecast)
print(type(max_hour_forecast))

# Create the db engine
engine = create_engine('sqlite:///weather_db.db')

# Store the dataframe as a table
data.to_sql(name='weather_table', con=engine, if_exists="replace")

# Query 1 on the relational table

n = 48

distinct_weather = pd.read_sql_query(
    sql=f"""
    SELECT DISTINCT
        city,
        weather,
        weather_description,
        ROUND(COUNT(weather)/{float(n)}*100, 0) AS percentage
    FROM weather_table
    WHERE hours_forecast <= {n}
    GROUP BY city, weather, weather_description;
    """,
    con=engine
)

print(f'Distinct weather by city for next {n} hours')
print(distinct_weather)


most_common_weather = pd.read_sql_query(
    sql=f"""
    SELECT
    city,
    weather,
    weather_description
    FROM (
        SELECT DISTINCT
            city,
            weather,
            weather_description,
        ROUND(COUNT(weather)/{float(n)}*100, 0) AS percentage
        FROM weather_table
        WHERE hours_forecast <= {n}
        GROUP BY city, weather, weather_description
    )
    GROUP BY city
    HAVING percentage == MAX(percentage);
    """,
    con=engine
)


print(f'Most common weather by city for next {n} hours')
print(most_common_weather)


temps = pd.read_sql_query(
    sql=f"""
    SELECT
    city,
    temp
    FROM weather_table
    WHERE hours_forecast <= {n};
    """,
    con=engine
)
print("temps")
print(temps)

average_temp = pd.read_sql_query(
    sql=f"""
    SELECT
    city,
    ROUND(AVG(temp), 2) AS average_temp
    FROM weather_table
    WHERE hours_forecast <= {n}
    GROUP BY city;
    """,
    con=engine
)

print(f'Average temp by city for next {n} hours')
print(average_temp)

highest_temp = pd.read_sql_query(
    sql=f"""
    SELECT
    datetime,
    city,
    MAX(temp) AS highest_temp
    FROM weather_table
    WHERE hours_forecast <= {n};
    """,
    con=engine
)

print(f'Highest temp city for next {n} hours')
print(highest_temp)

highest_variation_city = pd.read_sql_query(
    sql=f"""
    SELECT
        city,
        MAX(temp_variation)
    FROM (
        SELECT
            city,
            (MAX(temp) - MIN(temp)) AS temp_variation
        FROM weather_table
        WHERE hours_forecast <= {n}
        GROUP BY city
    );
    """,
    con=engine
)

print(f'Highest daily temp variation city for next {n} hours')
print(highest_variation_city)


strongest_wind_city = pd.read_sql_query(
    sql=f"""
    SELECT
        city,
        MAX(wind_speed_m_s)
    FROM weather_table
    WHERE hours_forecast <= {n};
    """,
    con=engine
)

print(f'Strongest wind city city for next {n} hours')
print(strongest_wind_city)
