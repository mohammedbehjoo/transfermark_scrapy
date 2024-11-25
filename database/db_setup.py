from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column, Integer, String, PrimaryKeyConstraint
from sqlalchemy.types import Integer, VARCHAR, CHAR, Float
import pandas as pd
import json


def cast_float(item):
    if "€" in item:
        item = item.replace("€", "")
        if "bn" in item:
            item = item.replace("bn", "")
            item = float(item)*1000000000
        elif "m" in item:
            item = item.replace("m", "")
            item = float(item)*1000000
        elif "k" in item:
            item = item.replace("k", "")
            item = float(item)*1000
        else:
            item = float(item)
    return item


# since some dataframes has many columns, set th max_columns to None.
pd.set_option("display.max_columns", None)

print(os.path.exists("config_db.env"))  # Should return True
# loading the env variables
load_dotenv("config_db.env")

leagues = os.getenv("leagues")
country_csv = os.getenv("country_csv")
teams = os.getenv("teams")
team_details = os.getenv("team_details")

# reading the env variables
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
port = int(port) if port else 0
database = os.getenv('DB_NAME')
if None in [username, password, host, port, database]:
    raise ValueError("One or more connection parameters are missing!")

# creating the engine for interactitng with the database
engine = create_engine(
    f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

metadata = MetaData()

# check if the engine is connected.
with engine.connect() as connection:
    result = connection.execute(
        text("SELECT version();"))  # Use text() for raw SQL
    for row in result:
        print(row)

# create the football schema
with engine.connect() as conn:
    try:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS football;"))
        conn.commit()
        print("football schema created")
    except:
        print("the football schema was already created")

inspector = inspect(engine)
schemas = inspector.get_schema_names()
print("schemas:\n", schemas)


# check if the country csv file exists
if country_csv and os.path.exists(country_csv):
    country_table = pd.read_csv(country_csv)
    print(country_table.head(3))
else:
    print("the country.csv file is either missing or empty")

country_table['country_name'] = country_table['country_name'].str.strip()
country_table = country_table.set_index("country_id")

print("country_table:\n", country_table)
print("country_table shape:\n", country_table.shape)


drop_query = text("DROP TABLE IF EXISTS football.countries CASCADE;")
create_query = text(
    '''
    CREATE TABLE football.countries (
        country_id SERIAL PRIMARY KEY,
        country_name VARCHAR(50) NOT NULL
    );
    '''
)

with engine.connect() as conn:
    conn.execute(drop_query)
    conn.execute(create_query)
    conn.commit()
    print("countries table recreated with primary key constraint")


# inserting values to country table
with engine.connect() as conn:
    country_table.to_sql(name="countries", con=conn, schema="football", if_exists="append", chunksize=50, method="multi",
                         index_label="country_id", dtype={"country_id": Integer(), "country_name": VARCHAR(50)})
    conn.commit()
    print("the data was inserted into countries table")


# working on leagues json file.
# read the lagues json file
with open(leagues, "r") as file:
    data = json.load(file)


# create a dataframe from leagues json file.
flattened_data = []
for country in data:
    country_name = country["country_name"]
    for league in country["leagues"]:
        league_data = {"country_name": country_name, **league}
        flattened_data.append(league_data)
df_leagues = pd.DataFrame(flattened_data)
# assign indexes as league_id for the database
df_leagues = df_leagues.assign(league_id=range(1, 6)).set_index("league_id")
# strip the string values of country_name key.
df_leagues['country_name'] = df_leagues['country_name'].str.strip()

# print the first 3 rows of leagues dataframe
print("leagues dataframe:\n", df_leagues.head(3))

# create a new dataframe to have the country_id for leagues, and remove the country_name from the leagues dataframe.
key_column = "country_name"
source_column = "country_id"

country_table_reset = country_table.reset_index()
merged_df = pd.merge(df_leagues, country_table_reset,
                     on=key_column, how="inner")
merged_df.drop("country_name", axis=1, inplace=True)
merged_df["total_value"] = merged_df["total_value"].apply(cast_float).astype("float64")
print("merged dataframe:\n", merged_df)
print("merged dataframe shape:\n", merged_df.shape)

drop_query = text("DROP TABLE IF EXISTS football.leagues CASCADE;")
# query for creating the leagues table in football schema
query_leagues = text('''
                   CREATE TABLE football.leagues(
                       league_id SERIAL PRIMARY KEY,
                       league_name VARCHAR(30) NOT NULL,
                       country_id INT NOT NULL,
                       league_url VARCHAR(255) NOT NULL,
                       club_num INT NOT NULL,
                       player_num INT NOT NULL,
                       total_value DOUBLE PRECISION NOT NULL,
                       FOREIGN KEY (country_id) REFERENCES football.countries(country_id)
                   )
                   ;''')

with engine.connect() as conn:
    conn.execute(drop_query)
    conn.execute(query_leagues)
    conn.commit()
    print("leagues table is recreated with primary and foreign keys")

with engine.connect() as conn:
    merged_df.to_sql(name="leagues", con=conn, schema="football", if_exists="append",
                     chunksize=50, method="multi", index_label="league_id", dtype={
                         "league_id": Integer(), "league_name": VARCHAR(30),
                         "country_id": Integer(), "league_url": VARCHAR(255),
                         "club_num": Integer(), "player_num": Integer(), "total_value": Float()
                     })
    conn.commit()
    print("data is inserted into leagues table.")
    

# read the teams json file
with open(teams,"r") as file:
    if teams and os.path.exists(teams):
        data=json.load(file)
        print("teams json file is read.")
    else:
        print("the teams file is either missing or empty")
    
# create a dataframe from teams json file. process the file to be ready for database insertion.
flattened_data=[]
for league in data:
    country_name=league["country_name"]
    league_name=league["league_name"]
    season=league["season"]
    for team in league["teams"]:
        team_data={"country_name":country_name,"league_name":league_name,"season":season,**team}
        flattened_data.append(team_data)
df_teams=pd.DataFrame(flattened_data)
df_teams.drop("country_name",axis=1,inplace=True)
# reset indexes of df_leagues file. we want to use it as a column.
df_leagues=df_leagues.reset_index()

# merge the df_teams, and df_leagues dataframes.
key_column="league_name"

merged_df=pd.merge(df_teams,df_leagues,on=key_column,how="inner")
merged_df.drop(["league_name","country_name","league_url","club_num","player_num","total_value"],inplace=True,axis=1)
merged_df["avg_market"]=merged_df["avg_market"].apply(cast_float).astype("float64")
merged_df["total_market"]=merged_df["total_market"].apply(cast_float).astype("float64")
# cast the dtype of these three columns to int.
merged_df['avg_age'] = pd.to_numeric(merged_df['avg_age'], errors='coerce').astype(int)
merged_df["squad_size"]=pd.to_numeric(merged_df["squad_size"],errors="coerce").astype(int)
merged_df["foreigners_num"]=pd.to_numeric(merged_df["foreigners_num"],errors="coerce").astype(int)

merged_df=merged_df.assign(team_id=range(0,merged_df.shape[0])).set_index("team_id")

# write the query to drop and create the teams table in the database.
drop_query=text('''
                drop table if exists footnall.teams;
                ''')

teams_query=text('''
                 create table if not exists football.teams(
                     team_id serial primary key,
                     league_id int not null,
                     team_name varchar(30) not null,
                     season int not null,
                     squad_size int not null,
                     avg_age int not null,
                     foreigners_num int not null,
                     avg_market double precision not null,
                     total_market double precision not null,
                     team_url varchar(255) not null,
                     foreign key (league_id) references football.leagues(league_id)
                     
                 )
                 ''')

with engine.connect() as conn:
    conn.execute(drop_query)
    conn.execute(teams_query)
    conn.commit()
    print("the teams table is recreated with primary and foreign keys contraint.")
    
# insert data into teams table
with engine.connect() as conn:
    merged_df.to_sql(name="teams",con=conn,schema="football",if_exists="append",
                     chunksize=50,method="multi",index_label="team_id",dtype={
                         "team_id":Integer(),"league_id":Integer(),"team_name":VARCHAR(30),
                         "season":Integer(),"squad_size":Integer(),"avg_age":Integer(),
                         "foreigners_num":Integer(),"avg_market":Float(),"total_market":Float(),"team_url":VARCHAR(255)
                     })
    conn.commit()
    print("data is inserted into the teams table.")