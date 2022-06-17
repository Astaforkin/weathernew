import psycopg2
import requests
from config import config

def create_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS weather (
                id serial PRIMARY KEY,
                city varchar(50) NOT NULL,
                date varchar(50) UNIQUE NOT NULL,
                weather varchar(50) UNIQUE NOT NULL,
                temp_max varchar(50) UNIQUE NOT NULL,
                temp_min varchar(50) UNIQUE NOT NULL
        )
        """,)
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



def insert_weather(date, weather, temp_max, temp_min):
    sql = "INSERT INTO weather (city, date, weather, temp_max, temp_min) VALUES('Ryazan',%s ,%s, %s, %s)"
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (date, weather, temp_max, temp_min,))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_table():
    url = f'https://www.7timer.info/bin/civillight.php?lon=39&lat=54&ac=0&unit=metric&output=json&tzshift=0'
    response = requests.get(url)
    if response.status_code == 200:
        response_data = response.json()
        for record in response_data['dataseries']:
            date = record['date']
            weather = record['weather']
            temp_max = record['temp2m']['max']
            temp_min = record['temp2m']['min']
            insert_weather(date, weather, temp_max, temp_min)
                       
def update_table():
    sql = "ALTER TABLE weather DROP CONSTRAINT weather_temp_min_key;"
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql,)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    

if __name__ == '__main__':
    update_table()