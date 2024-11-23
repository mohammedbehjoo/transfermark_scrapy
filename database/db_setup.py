from dotenv import load_dotenv
import os
from sqlalchemy import create_engine,text,inspect
from sqlalchemy.types import Integer,VARCHAR,CHAR
import pandas as pd
import json

pd.set_option("display.max_columns",None)

load_dotenv("config.env")
leagues=os.getenv("leagues")
country_csv=os.getenv("country_csv")
teams=os.getenv("teams")
team_details=os.getenv("team_details")