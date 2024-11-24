from dotenv import load_dotenv
import os
from sqlalchemy import create_engine,text,inspect
from sqlalchemy.types import Integer,VARCHAR,CHAR
import pandas as pd
import json

def cast_float(item):
    if "€" in item:
        item=item.replace("€","")
        if "bn" in item: 
            item=item.replace("bn","")
            item=float(item)*1000000000
        elif "m" in item:
            item=item.replace("m","")
            item=float(item)*1000000
        elif "k" in item:
            item=item.replace("k","")
            item=float(item)*1000
        else:
            item=float(item)
    return item

# since some dataframes has many columns, set th max_columns to None.
pd.set_option("display.max_columns",None)

print(os.path.exists("config_db.env"))  # Should return True
# loading the env variables
load_dotenv("config_db.env")

leagues=os.getenv("leagues")
country_csv=os.getenv("country_csv")
teams=os.getenv("teams")
team_details=os.getenv("team_details")

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
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

# check if the engine is connected.
with engine.connect() as connection:
    result = connection.execute(text("SELECT version();"))  # Use text() for raw SQL
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

inspector=inspect(engine)
schemas=inspector.get_schema_names()
print("schemas:\n",schemas)


# check if the country csv file exists
if country_csv and os.path.exists(country_csv):
    country_table=pd.read_csv(country_csv)
    print(country_table.head(3))
else:
    print("the country.csv file is either missing or empty")
    
country_table=country_table.set_index("country_id")
print("country_table:\n",country_table)
print("country_table shape:\n",country_table.shape)

query_country=text(
    '''
    create table if not exists football.country(
        country_id serial primary key,
        country_name varchar(50) not null
    );
    '''
)

# creating the country table in football schema
with engine.connect() as conn:
    try:
        conn.execute(query_country)
        conn.commit()
        print("country table was created")
    except:
        print("country table was already created")

# inserting values to country table
with engine.connect() as conn:
    country_table.to_sql(name="country",con=conn,schema="football",if_exists="replace",chunksize=50,method="multi",
                         index_label="country_id",dtype={"country_id":Integer(),"country_name":VARCHAR(50)})
    conn.commit()
    print("the data was inserted into country table")


# working on leagues json file.
# read the lagues json file
with open(leagues,"r") as file:
    data=json.load(file)


# create a dataframe from leagues json file.
flattened_data=[]
for country in data:
    country_name=country["country_name"]
    for league in country["leagues"]:
        league_data={"country_name":country_name,**league}
        flattened_data.append(league_data)
df_leagues=pd.DataFrame(flattened_data)
# assign indexes as league_id for the database
df_leagues=df_leagues.assign(league_id=range(1,6)).set_index("league_id")
# strip the string values of country_name key.
df_leagues['country_name'] = df_leagues['country_name'].str.strip()

# print the first 3 rows of leagues dataframe
print("leagues dataframe:\n",df_leagues.head(3))


