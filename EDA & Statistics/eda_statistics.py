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
# directory for saving txt file
save_txt_dir=os.getenv("save_dir_txt")
# create the directory if it does not exists
os.makedirs(save_txt_dir,exist_ok=True)

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
txt_file_path=os.path.join(save_txt_dir,"results_df_league.txt")
with open(txt_file_path,"a") as file:
    file.write("Describe df_leagues dataframe:\n")
    file.write(df_leagues.describe().to_string())
    file.write("\n"+"-"*30+"\n")

print(f"df_leagues number.describe() is written to the file {txt_file_path}."+"\n"+"-"*30+"\n")


# checking for missing values
print(f"null values of df_leagues:\n{df_leagues.isnull().sum()}"+"\n"+"-"*30+"\n")

# write the df_leagues null values count to a txt file.
with open(txt_file_path,"a") as file:
    file.write("Number of null values of df_leagues dataframe columns:\n")
    file.write(df_leagues.isnull().sum().to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_leagues number of null values is written to the file {txt_file_path}."+"\n"+"-"*30+"\n")


# distributions
# histogram for distributions
for col in ["club_num","player_num","total_value"]:
    plt.figure(figsize=(6,4))
    sns.histplot(df_leagues[col], kde=True, bins=10, color='blue')
    plt.title(f"Distirbution of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    fig_file_path=os.path.join(save_figure_dir,f"Disribution of {col}.jpg")
    plt.savefig(fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {fig_file_path}"+"\n"+"-"*30+"\n")

# check skewness numerically
print(f"skewness\n{df_leagues[['club_num','player_num','total_value']].skew()}"+"\n"+"-"*30+"\n")

# write the df_leagues skewness to a txt file.
with open(txt_file_path,"a") as file:
    file.write("skewness of df_leagues dataframe columns:\n")
    file.write(df_leagues[["club_num","player_num","total_value"]].skew().to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_leagues skewness of columns is written to the file {txt_file_path}.","\n"+"-"*30+"\n")

# correlations
# correlation matrix
correlation_matrix=df_leagues[["club_num","player_num","total_value"]].corr()
print(f"Correlation matrix:\n{correlation_matrix}"+"\n"+"-"*30+"\n")
# write the df_leagues correlation matrix to a txt file.
with open(txt_file_path,"a") as file:
    file.write("correlation matrix of df_leagues dataframe columns:\n")
    file.write(correlation_matrix.to_string())
    file.write("\n"+"-"*30+"\n")
print(f"df_leagues correlation matrix is written to the file {txt_file_path}."+"\n"+"-"*30+"\n")

# heatmap to visualize correlations
# correlation matrix
plt.figure(figsize=(8,6))
sns.heatmap(correlation_matrix,annot=True,cmap="coolwarm",fmt=".2f")
plt.title("Correlation matrix")
fig_file_path=os.path.join(save_figure_dir,"correlation_matrix_df_leagues.jpg")
plt.savefig(fig_file_path,format="jpg")
plt.close()
print(f"Figure is saved at: {fig_file_path}"+"\n"+"-"*30+"\n")

# outliers detection
# box plots to identify outliers
# box plot for each numeric column
for col in ["total_value","player_num","club_num"]:
    plt.figure(figsize=(6,4))
    sns.boxplot(y=df_leagues[col],color="skyblue")
    plt.title(f"Boxplot of {col}")
    plt.ylabel(col)
    fig_file_path=os.path.join(save_figure_dir,f"boxplot of {col}.jpg")
    plt.savefig(fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {fig_file_path}"+"\n"+"-"*30+"\n")

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
    with open(txt_file_path,"a") as file:
        file.write(f"outliers of df_leagues dataframe of col {col}:\n")
        file.write(outliers.to_string())
        file.write("\n"+"-"*30+"\n")
    print(f"df_leagues outliers are written to the file {txt_file_path}."+"\n"+"-"*30+"\n")
    
# calculate range for each numeric value
for col in ["total_value","player_num","club_num"]:
    col_min=df_leagues[col].min()
    col_max=df_leagues[col].max()
    col_range= col_max - col_min

    print(f"{col} -> Min: {col_min}, Max: {col_max}, Range: {col_range}"+"\n"+"-"*30+"\n")
    # write the df_leagues correlation matrix to a txt file.
    with open(txt_file_path,"a") as file:
        file.write(f"df_leagues dataframe range of col {col}:\n")
        file.write("Min: "+str(col_min)+"\n")
        file.write("Max: "+str(col_max)+"\n")
        file.write("Range: "+str(col_range)+"\n")
        file.write("\n"+"-"*30+"\n")
    print(f"df_leagues range of column {col} is written to the file {txt_file_path}."+"\n"+"-"*30+"\n")

# barplot to compare league values for each numeric column
for col in ['club_num', 'player_num', 'total_value']:
    plt.figure(figsize=(8,6))
    sns.barplot(x="league_name",y=col,data=df_leagues,palette="viridis")
    plt.title(f"Comparison of {col} across leagues")
    plt.xlabel("League")
    plt.ylabel(col)
    plt.xticks(rotation=45)
    fig_file_path=os.path.join(save_figure_dir,f"Comparison of {col} across leagues.jpg")
    plt.savefig(fig_file_path,format="jpg")
    plt.close()
    print(f"Figure is saved at: {fig_file_path}"+"\n"+"-"*30+"\n")
    
