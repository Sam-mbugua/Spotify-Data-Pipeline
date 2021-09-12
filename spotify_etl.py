import sqlalchemy
import pandas as pd
import requests
import json 
from datetime import datetime
import datetime
import sqlite3


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # check if Datafram eis empty
    if df.empty:
        print("No songs download. Finishing excution")
        return False

    # Primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Null check
    if df.isnull().values.any():
        raise Exception("Null value found")

    # Check that all timestamps are of yesterday's date 
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")

    return True


def run_spotify_etl():

    database_location = "sqlite:///my_played_tracks.sqlite"
    token = """BQAgqNrJOgXPtF4xlQsbdjWEPOo0UaBvyWi2JVbdpFmGwcg4rutKbE29SWuRRVbLE_
    fu5GkHhW1Z3k7OwRjMotO5NCAdRaGqhoysFUmToHU1edTdlFCyEIasLLbG3Z0xcIDJk697yhi2FJ9j2YcPUg3ffFV8C8SoSACk"""


    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers = headers)
    
    data = req.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name","artist_name","played_at","timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        print("Data is Valid")


    #Load
    engine = sqlalchemy.create_engine(database_location)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)

    try:
        song_df.to_sql("my_played_tracks",engine,index=False,if_exists='append')
    except:
        print("Data already exists in the database")
