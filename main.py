import psycopg2
import requests
from config import config


def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS weather (
                id serial PRIMARY KEY,
                city varchar(50) NOT NULL,
                date varchar(50) NOT NULL,
                weather varchar(50) NOT NULL,
                temp_max varchar(50) NOT NULL,
                temp_min varchar(50) NOT NULL,
                UNIQUE (city, date)
        )
        """,
    )
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def run_sql(statement, arguments):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(statement, arguments)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_data_from_weather_api(lon, lat):
    url = f"https://www.7timer.info/bin/civillight.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0"
    response = requests.get(url)
    if response.status_code == 200:
        response_data = response.json()
        return response_data["dataseries"]


def insert_data_into_db(cityname, weather_data):
    for record in weather_data:
        statement = "INSERT INTO weather (city, date, weather, temp_max, temp_min) VALUES(%(city)s ,%(date)s ,%(weather)s, %(temp_max)s, %(temp_min)s) ON CONFLICT (city, date) DO UPDATE SET weather=EXCLUDED.weather, temp_max=EXCLUDED.temp_max, temp_min=EXCLUDED.temp_min"
        weatherdict = dict(
            city=cityname,
            date=record["date"],
            weather=record["weather"],
            temp_max=record["temp2m"]["max"],
            temp_min=record["temp2m"]["min"],
        )
        run_sql(statement, weatherdict)



cities = [
    {"name": "Ryazan", "lon": "39", "lat": "54"},
    {"name": "Moscow", "lon": "37.36", "lat": "54.44"},
    {"name": "St.Petersburg", "lon": "30.19", "lat": "59.57"}
]


if __name__ == "__main__":
    for city in cities:
        weather_data = get_data_from_weather_api(city['lon'], city['lat'])
        insert_data_into_db(city['name'], weather_data)