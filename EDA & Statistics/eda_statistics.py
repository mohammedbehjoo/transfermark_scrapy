from dotenv import load_dotenv
import os
from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
from scipy.stats import f_oneway,ttest_ind

# a function for deleting every resul txt file in the eda directory.
def delete_txt_files(directory):
    path=Path(directory)
    for txt_file in path.rglob("*.txt"):
        txt_file.unlink()
        print(f"Deleted: {txt_file}")

# a function to process and cast items of columns to float datatype
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

# root folder for the output
root_dir=os.getenv("save_dir_txt")

# directory for saving leagues figures
save_figure_leagues_dir=os.getenv("save_dir_figures")
save_figure_leagues_dir=os.path.join(save_figure_leagues_dir,"leagues")
# Create the directory if it does not exist
os.makedirs(save_figure_leagues_dir, exist_ok=True)
print("save figure leagues directory:\n",save_figure_leagues_dir)

# directory for saving leagues figures
save_figure_teams_dir=os.getenv("save_dir_figures")
save_figure_teams_dir=os.path.join(save_figure_teams_dir,"teams")
# Create the directory if it does not exist
os.makedirs(save_figure_teams_dir, exist_ok=True)
print("save figure teams directory:\n",save_figure_teams_dir)

# directory for saving leagues txt file
save_txt_leagues_dir=os.getenv("save_dir_txt")
save_txt_leagues_dir=os.path.join(save_txt_leagues_dir,"leagues")
# create the directory if it does not exists
os.makedirs(save_txt_leagues_dir,exist_ok=True)
print("save txt leagues directory:\n",save_txt_leagues_dir)

# directory for saving teams txt file
save_txt_teams_dir=os.getenv("save_dir_txt")
save_txt_teams_dir=os.path.join(save_txt_teams_dir,"teams")
# create the teams directory if it does not exists
os.makedirs(save_txt_teams_dir,exist_ok=True)
print("save txt teams directory:\n",save_txt_teams_dir)

# leagues txt file path
leagues_txt_file_path=os.path.join(save_txt_leagues_dir,"results_df_league.txt")
# teams txt file path
teams_txt_file_path=os.path.join(save_txt_teams_dir,"teams.txt")

# delete the txt files before starting. this makes sure that evert txt file is being made from scratch.
delete_txt_files(root_dir)


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
print(f"describe df_leagues dataframe:\n{df_leagues.describe()}"+"\n"+"-"*30+"\n")

# write the df_leagues.describe() to a txt file.

with open(leagues_txt_file_path,"a") as file:
    file.write("Describe df_leagues dataframe:\n")
    file.write(df_leagues.describe().to_string())
    file.write("\n"+"-"*30+"\n")

print(f"df_leagues number.describe() is written to the file {leagues_txt_file_path}."+"\n"+"-"*30+"\n")


# checking for missing values
print(f"null values of df_leagues:\n{df_leagues.isnull().sum()}"+"\n"+"-"*30+"\n")

# write the df_leagues null values count to a txt file.
with open(leagues_txt_file_path,"a") as file:
    file.write("Number of null values of df_leagues dataframe columns:\n")
    file.write(df_leagues.isnull().sum().to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_leagues number of null values is written to the file {leagues_txt_file_path}."+"\n"+"-"*30+"\n")


# distributions
# histogram for distributions
for col in ["club_num","player_num","total_value"]:
    plt.figure(figsize=(6,4))
    sns.histplot(df_leagues[col], kde=True, bins=10, color='blue')
    plt.title(f"Distirbution of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    leagues_fig_file_path=os.path.join(save_figure_leagues_dir,f"Disribution of {col}.jpg")
    plt.savefig(leagues_fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# check skewness numerically
print(f"skewness\n{df_leagues[['club_num','player_num','total_value']].skew()}"+"\n"+"-"*30+"\n")

# write the df_leagues skewness to a txt file.
with open(leagues_txt_file_path,"a") as file:
    file.write("skewness of df_leagues dataframe columns:\n")
    file.write(df_leagues[["club_num","player_num","total_value"]].skew().to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_leagues skewness of columns is written to the file {leagues_txt_file_path}.","\n"+"-"*30+"\n")

# correlations
# correlation matrix
correlation_matrix=df_leagues[["club_num","player_num","total_value"]].corr()
print(f"Correlation matrix:\n{correlation_matrix}"+"\n"+"-"*30+"\n")
# write the df_leagues correlation matrix to a txt file.
with open(leagues_txt_file_path,"a") as file:
    file.write("correlation matrix of df_leagues dataframe columns:\n")
    file.write(correlation_matrix.to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_leagues correlation matrix is written to the file {leagues_txt_file_path}."+"\n"+"-"*30+"\n")

# heatmap to visualize correlations
# correlation matrix
plt.figure(figsize=(8,6))
sns.heatmap(correlation_matrix,annot=True,cmap="coolwarm",fmt=".2f")
plt.title("Correlation matrix")
leagues_fig_file_path=os.path.join(save_figure_leagues_dir,"correlation_matrix_df_leagues.jpg")
plt.savefig(leagues_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# outliers detection
# box plots to identify outliers
# box plot for each numeric column
for col in ["total_value","player_num","club_num"]:
    plt.figure(figsize=(6,4))
    sns.boxplot(y=df_leagues[col],color="skyblue")
    plt.title(f"Boxplot of {col}")
    plt.ylabel(col)
    leagues_fig_file_path=os.path.join(save_figure_leagues_dir,f"boxplot of {col}.jpg")
    plt.savefig(leagues_fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# detecting outliers using IQR
for col in ["total_value","player_num","club_num"]:
    Q1=df_leagues[col].quantile(0.25)
    Q3=df_leagues[col].quantile(0.75)
    IQR=Q3-Q1 #interquartile range
    
    lower_bound=Q1-1.5*IQR
    upper_bound=Q3+1.5*IQR
    
    outliers=df_leagues[(df_leagues[col]<lower_bound) | (df_leagues[col] > upper_bound) ]
    print(f"\nOutliers in {col}\n",outliers,"\n","-"*30,"\n")
    # write the df_leagues coutliers using IQR to a txt file.
    with open(leagues_txt_file_path,"a") as file:
        file.write(f"outliers of df_leagues dataframe of col {col}:\n")
        file.write(outliers.to_string())
        file.write("\n"+"-"*30+"\n")
    print(f"df_leagues outliers are written to the file {leagues_txt_file_path}."+"\n"+"-"*30+"\n")
    
# calculate range for each numeric value
for col in ["total_value","player_num","club_num"]:
    col_min=df_leagues[col].min()
    col_max=df_leagues[col].max()
    col_range= col_max - col_min

    print(f"{col} -> Min: {col_min}, Max: {col_max}, Range: {col_range}"+"\n"+"-"*30+"\n")
    # write the df_leagues correlation matrix to a txt file.
    with open(leagues_txt_file_path,"a") as file:
        file.write(f"df_leagues dataframe range of col {col}:\n")
        file.write("Min: "+str(col_min)+"\n")
        file.write("Max: "+str(col_max)+"\n")
        file.write("Range: "+str(col_range)+"\n")
        file.write("\n"+"-"*30+"\n")
    print(f"df_leagues range of column {col} is written to the file {leagues_txt_file_path}."+"\n"+"-"*30+"\n")

# barplot to compare league values for each numeric column
for col in ['club_num', 'player_num', 'total_value']:
    plt.figure(figsize=(8,6))
    sns.barplot(x="league_name",y=col,data=df_leagues,palette="viridis")
    plt.title(f"Comparison of {col} across leagues")
    plt.xlabel("League")
    plt.ylabel(col)
    plt.xticks(rotation=45)
    leagues_fig_file_path=os.path.join(save_figure_leagues_dir,f"Comparison of {col} across leagues.jpg")
    plt.savefig(leagues_fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")
    
# summary metric for each numeric column
for col in ['club_num', 'player_num', 'total_value']:
    print(f"Summary for {col}:")
    print(f"Mean: {df_leagues[col].mean():.2f}")
    print(f"Median: {df_leagues[col].median():.2f}")
    print(f"Mode: {df_leagues[col].mode().iloc[0]:.2f}")
    print(f"Standard deviation: {df_leagues[col].std():.2f}")
    print("-"*30)
    with open(leagues_txt_file_path,"a") as file:
        file.write(f"Summary for {col}:"+"\n")
        file.write(f"Mean: {df_leagues[col].mean():.2f}"+"\n")
        file.write(f"Median: {df_leagues[col].median():.2f}"+"\n")
        file.write(f"Mode: {df_leagues[col].mode().iloc[0]:.2f}"+"\n")
        file.write(f"Standard deviation: {df_leagues[col].std():.2f}"+"\n")
        file.write("-"*30+"\n")
        print(f"df_leagues summary metrics of column {col} are written to the file {leagues_txt_file_path}."+"\n"+"-"*30+"\n")
    
# visualization of summary metrics
for col in ['club_num', 'player_num', 'total_value']:
    plt.figure(figsize=(6,4))
    sns.boxplot(y=df_leagues[col],color="skyblue")
    
    # add mean and median lines
    plt.axhline(df_leagues[col].mean(),color="green",linestyle="--",label=f"Mean: {df_leagues[col].mean():.2f}")
    plt.axhline(df_leagues[col].median(),color="orange",linestyle="-",label=f"Median: {df_leagues[col].median():.2f}")
    
    plt.title(f"{col} with mean and median")
    plt.ylabel(col)
    plt.legend()
    leagues_fig_file_path=os.path.join(save_figure_leagues_dir,f"{col} with mean and meadian.jpg")
    plt.savefig(leagues_fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# visualize overall ranges
# calculate ranges
ranges=df_leagues[["club_num","player_num","total_value"]].max() - df_leagues[["club_num","player_num","total_value"]].min()

# plot ranges
plt.figure(figsize=(8,5))
sns.barplot(x=ranges.values,y=ranges.index,palette="coolwarm")
plt.title("Range of numeric columns")
plt.xlabel("Range")
plt.ylabel("Column")
leagues_fig_file_path=os.path.join(save_figure_leagues_dir,"range of numeric columns.jpg")
plt.savefig(leagues_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# scatter plot: Club_num vs. total_value
plt.figure(figsize=(8,6))
sns.scatterplot(x="club_num",y="total_value",data=df_leagues,hue="league_name",palette="Set2",s=100)
plt.title("Club number vs. Total value",fontsize=16)
plt.xlabel("Number of clubs",fontsize=12)
plt.ylabel("Total value",fontsize=12)
plt.legend(title="League name",bbox_to_anchor=(1.05,1),loc="upper left")
plt.grid(True)
plt.tight_layout()
leagues_fig_file_path=os.path.join(save_figure_leagues_dir,"Club number vs. Total value.jpg")
plt.savefig(leagues_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")


# Scatter plot: player_num vs. total_value
plt.figure(figsize=(8, 6))
sns.scatterplot(x='player_num', y='total_value', data=df_leagues, hue='league_name', palette='Set2', s=100)
plt.title('Player Number vs. Total Value', fontsize=16)
plt.xlabel('Number of Players', fontsize=12)
plt.ylabel('Total Value', fontsize=12)
plt.legend(title='League Name', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
leagues_fig_file_path=os.path.join(save_figure_leagues_dir,"Player Number vs. Total Value.jpg")
plt.savefig(leagues_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# pie charts
# categorical columns for leagues
df_leagues["league_name"]=["Premier League","Ligue 1","Bundesliga","Serie A","LaLiga"]

# pie chart for player_num
plt.figure(figsize=(8,8))
plt.pie(
    df_leagues["player_num"],
    labels=df_leagues["league_name"],
    autopct="%1.1f%%", # display percentage
    startangle=90,
    colors=plt.cm.Paired.colors,    
)
plt.title("Prportion of players by league")
leagues_fig_file_path=os.path.join(save_figure_leagues_dir,"Prportion of players by league.jpg")
plt.savefig(leagues_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")

# pie chart for total_value
plt.figure(figsize=(8,8))
plt.pie(df_leagues["total_value"],
        labels=df_leagues["league_name"],
        autopct="%1.1f%%",
        startangle=90,
        colors=plt.cm.Paired.colors)
plt.title("Proportion of total value by league")
leagues_fig_file_path=os.path.join(save_figure_leagues_dir,"Proportion of total value by league.jpg")
plt.savefig(leagues_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {leagues_fig_file_path}"+"\n"+"-"*30+"\n")



# ---------------------------
# ---------------------------
# teams dataframe

# read the teams json file
with open(teams,"r") as file:
    if teams and os.path.exists(teams):
        data=json.load(file)
        print("teams json file is read.")
    else:
        print("the teams file is either missing or empty")
        
flattened_data=[]
for league in data:
    country_name=league["country_name"]
    league_name=league["league_name"]
    season=league["season"]
    for team in league["teams"]:
        team_data={"country_name":country_name,"league_name":league_name,"season":season,**team}
        flattened_data.append(team_data)
df_teams=pd.DataFrame(flattened_data)

# cast dtypes to the appropriate dtypes
df_teams["squad_size"]=pd.to_numeric(df_teams["squad_size"],errors="coerce")
df_teams["avg_age"]=pd.to_numeric(df_teams["avg_age"],errors="coerce")
df_teams["foreigners_num"]=pd.to_numeric(df_teams["foreigners_num"],errors="coerce")
df_teams["avg_market"]=df_teams["avg_market"].apply(cast_float)
df_teams["total_market"]=df_teams["total_market"].apply(cast_float)

# basic summary statistics
print(f"describe df_teams dataframe:\n{df_teams.describe()}"+"\n"+"-"*30+"\n")

# write the df_teams.describe() to a txt file.
with open(teams_txt_file_path,"a") as file:
    file.write("Describe df_teams dataframe:\n")
    file.write(df_teams.describe().to_string())
    file.write("\n"+"-"*30+"\n")

print(f"df_teams.describe() is written to the file {teams_txt_file_path}."+"\n"+"-"*30+"\n")

print(f"null values of df_leagues:\n{df_teams.isnull().sum()}"+"\n"+"-"*30+"\n")

# write the df_leagues null values count to a txt file.
with open(teams_txt_file_path,"a") as file:
    file.write("Number of null values of df_teams dataframe columns:\n")
    file.write(df_teams.isnull().sum().to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_teams number of null values is written to the file {teams_txt_file_path}."+"\n"+"-"*30+"\n")

# distributions
# histogram for distributions
for col in ["squad_size","avg_age","foreigners_num","avg_market","total_market"]:
    plt.figure(figsize=(6,4))
    sns.histplot(df_teams[col], kde=True, bins=10, color='blue')
    plt.title(f"Distirbution of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    teams_fig_file_path=os.path.join(save_figure_teams_dir,f"Disribution of {col}.jpg")
    plt.savefig(teams_fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {teams_fig_file_path}"+"\n"+"-"*30+"\n")

# check skewness numerically
print(f"skewness\n{df_teams[['squad_size','avg_age','foreigners_num','avg_market','total_market']].skew()}"+"\n"+"-"*30+"\n")

# write the df_teams skewness to a txt file.
with open(teams_txt_file_path,"a") as file:
    file.write("skewness of df_teams dataframe columns:\n")
    file.write(df_teams[["squad_size","avg_age","foreigners_num","avg_market","total_market"]].skew().to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_teams skewness of columns is written to the file {teams_txt_file_path}.","\n"+"-"*30+"\n")

# Compute correlation matrix of df_teams
correlation_matrix = df_teams[["squad_size","avg_age","foreigners_num","avg_market","total_market"]].corr()

# Display the correlation matrix of df_teams
print(f"Correlation matrix of df_teams:\n{correlation_matrix}"+"\n"+"-"*30+"\n")

# write the df_teams correlation matrix to a txt file.
with open(teams_txt_file_path,"a") as file:
    file.write("correlation matrix of df_teams dataframe columns:\n")
    file.write(correlation_matrix.to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_teams correlation matrix is written to the file {teams_txt_file_path}."+"\n"+"-"*30+"\n")

# plot the correlation heatmap
plt.figure(figsize=(10,8))
sns.heatmap(correlation_matrix,annot=True,cmap="coolwarm",fmt=".2f",linewidths=0.5)
plt.title("Correlation matrix of numerical features")
teams_fig_file_path=os.path.join(save_figure_teams_dir,"Correlation matrix of numerical features.jpg")
plt.savefig(teams_fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {teams_fig_file_path}"+"\n"+"-"*30+"\n")

