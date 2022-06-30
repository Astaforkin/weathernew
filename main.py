from mysqlx import Statement
import psycopg2
import requests
from config import config


def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS cities (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            lon FLOAT NOT NULL,
            lat FLOAT NOT NULL,
            UNIQUE (name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS weather (
                id SERIAL PRIMARY KEY,
                city_id INTEGER NOT NULL,
                date date NOT NULL,
                weather varchar(50) NOT NULL,
                temp_max INTEGER NOT NULL,
                temp_min INTEGER NOT NULL,
                UNIQUE (city_id, date),
                FOREIGN KEY (city_id)
                REFERENCES cities (id)
        )
        """,
    )
    for command in commands:
        run_sql(command)


def run_sql(statement, argument=None):
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(statement, argument)
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
        statement = "INSERT INTO weather (city_id, date, weather, temp_max, temp_min) VALUES( %(city_id)s, to_date('%(date)s', 'YYYYMMDD'), %(weather)s, %(temp_max)s, %(temp_min)s) ON CONFLICT (city_id, date) DO UPDATE SET weather=EXCLUDED.weather, temp_max=EXCLUDED.temp_max, temp_min=EXCLUDED.temp_min"
        weatherdict = dict(
            city_id=cityname,
            date=record["date"],
            weather=record["weather"],
            temp_max=record["temp2m"]["max"],
            temp_min=record["temp2m"]["min"],
        )
        run_sql(statement, weatherdict)


def insert_data_cities_list_into_db():
    for city in cities:
        statement = "INSERT INTO cities (name, lon, lat) VALUES(%(name)s ,%(lon)s ,%(lat)s) ON CONFLICT (name) DO UPDATE SET lon=EXCLUDED.lon, lat=EXCLUDED.lat RETURNING id"
        citylist = dict(name=city["name"], lon=city["lon"], lat=city["lat"])
        run_sql(statement, citylist)

def get_city_id():
    commands = "SELECT id FROM cities"
    for command in commands:
        run_sql(command)

cities = [
    {"name": "Ryazan", "lon": "39", "lat": "54"},
    {"name": "Moscow", "lon": "37.36", "lat": "54.44"},
    {"name": "St.Petersburg", "lon": "30.19", "lat": "59.57"},
]


if __name__ == "__main__":
    city_id = get_city_id()
    for id in city_id:
        for city in cities:
            weather_data = get_data_from_weather_api(city["lon"], city["lat"])
            insert_data_into_db(id, weather_data)
    