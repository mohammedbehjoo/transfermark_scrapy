from dotenv import load_dotenv
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
from scipy.stats import f_oneway,ttest_ind

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


print(os.path.exists("config.env")) 

# loading the env variables
load_dotenv("config.env")

leagues = os.getenv("leagues")
country_csv = os.getenv("country_csv")
teams = os.getenv("teams")
team_details = os.getenv("team_details")
# directory for saving figures
save_figure_dir=os.getenv("save_dir_figures")
# Create the directory if it does not exist
os.makedirs(save_figure_dir, exist_ok=True)

# reading the lagues json file
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
# cast the datatype of total_value column to float
df_leagues["total_value"]=df_leagues["total_value"].apply(cast_float)
# cast the club_num, and player_num to numeric dtype
df_leagues["club_num"]=pd.to_numeric(df_leagues["club_num"],errors="coerce")
df_leagues["player_num"]=pd.to_numeric(df_leagues["player_num"],errors="coerce")

# basic summary statistics
print(f"describe df_leagues dataframe:\n{df_leagues.describe()}","\n","-"*30)

# write the df_leagues.describe() to a txt file.
with open("results_df_league.txt","a") as file:
    file.write("Describe df_leagues dataframe:\n")
    file.write(df_leagues.describe().to_string())
    file.write("\n"+"-"*30+"\n")

print("df_leagues number.describe() is written to the file.")


# checking for missing values
print(f"null values of df_leagues:\n{df_leagues.isnull().sum()}"+"\n"+"-"*30+"\n")

# write the df_leagues null values count to a txt file.
with open("results_df_league.txt","a") as file:
    file.write("Number of null values of df_leagues dataframe columns:\n")
    file.write(df_leagues.isnull().sum().to_string())
    file.write("\n"+"-"*30+"\n")
print("df_leagues number of null values is written to the file.")


# distributions
# histogram for distributions
for col in ["club_num","player_num","total_value"]:
    plt.figure(figsize=(6,4))
    sns.histplot(df_leagues[col], kde=True, bins=10, color='blue')
    plt.title(f"Distirbution of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    file_path=os.path.join(save_figure_dir,f"Disribution of {col}.jpg")
    plt.savefig(file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {file_path}")