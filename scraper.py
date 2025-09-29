# Dependancies

import pandas as pd
import datetime
from random import randint
from time import sleep
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta

# Use _print instead of print to print with a timestamp.
def _print(*args, **kw):
    print("[%s]" % (datetime.now()),*args, **kw)

    # Free Agent List Generator

from selenium import webdriver
import pandas as pd
import time
from webdriver_manager.chrome import ChromeDriverManager
import os
from io import StringIO

# element_url will be the path to the table itself
ffa_url = 'https://fantasy.espn.com/hockey/players/add?leagueId=190894274'
element_url = ''

driver = webdriver.Chrome()
driver.implicitly_wait(30)

output_path_ffa = 'freeagents.csv'
# Webdriver automation runs the fantasy page (link below), goes to the xpath of the table as defined (cunt to find) then exports it to a table.
# Currently only works for one page of agents but I think we can fix that
driver.get(ffa_url)

# Try to read how many pages of free agents there are
page_ct = driver.find_element("xpath", '/html/body/div[1]/div[1]/div/div/div[5]/div[2]/div[3]/div/div/div[2]/nav/div/ul/li[7]/a')
text = page_ct.text

## Skaters
#Find the "Next page" button, and click it after 3 seconds. Loops 25 times
for loop_count in range(25):
    #print('Awake - Skater', loop_count)
    time.sleep(3)
    try:
        element_url = driver.find_element("xpath", '//*[@id="fitt-analytics"]/div/div[5]/div[2]/div[3]/div/div/div[2]/div/div/table[1]').get_attribute('outerHTML')
        element_io = StringIO(element_url)
        # Rip the table, header 1 cuts the bullshit double header off ESPNs table
        ffa_df=pd.read_html(element_io, header=1)[0]
        # Print to the csv
        ffa_df.to_csv(output_path_ffa, mode='a',header = not os.path.exists(output_path_ffa), index=False)
        # Click the next page
        driver.find_element("xpath", '//*[@id="fitt-analytics"]/div/div[5]/div[2]/div[3]/div/div/div[2]/nav/button[2]').click()
    except:
        print("Exception")

## Goalies
time.sleep(2)

goalie_element = driver.find_element("xpath", '//*[@id="filterSlotIds"]/label[4]')
driver.execute_script("arguments[0].click();", goalie_element)

#Find the "Next page" button, and click it after 3 seconds. Loops 5 times
for loop_count_g in range(5):
    #print('Awake - Goalie', loop_count_g)
    time.sleep(3)
    try:
        element_url = driver.find_element("xpath", '//*[@id="fitt-analytics"]/div/div[5]/div[2]/div[3]/div/div/div[2]/div/div/table[1]').get_attribute('outerHTML')
        element_io = StringIO(element_url)
        # Rip the table, header 1 cuts the bullshit double header off ESPNs table
        ffa_df=pd.read_html(element_io, header=1)[0]
        # Print to the csv
        ffa_df.to_csv(output_path_ffa, mode='a',header = not os.path.exists(output_path_ffa), index=False)
        # Click the next page
        driver.find_element("xpath", '//*[@id="fitt-analytics"]/div/div[5]/div[2]/div[3]/div/div/div[2]/nav/button[2]').click()
    except:
        print("Exception")

print('Complete')

# Player Index
import pandas as pd
import datetime
from random import randint
from time import sleep
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

index_url = 'https://www.naturalstattrick.com/playerlist.php?fromseason=20232024&thruseason=20242025&stype=2&sit=all&stdoi=std&rate=n'
output_path_index = 'index.csv'
columns_index = ['Player', 'Position', 'Team']

index_dfs = pd.read_html(index_url)
index_df = index_dfs[0]
index_dff = pd.DataFrame(index_df, columns=columns_index)
index_dff.to_csv(output_path_index, mode='w', header = True, index = False)
print('Complete')

# Grabs the schedule, then drops all the duplicate dates and makes a list of every day a game is played. Currently unused.
url = 'https://www.hockey-reference.com/leagues/NHL_2022_games.html'
dfs = pd.read_html(url)
df = dfs[0]
dates = pd.to_datetime(df['Date'], format="%Y-%m-%d").dt.date
dates = pd.Series(dates).drop_duplicates().tolist()
print('complete')

# Define lists for what strengths to use (Even strength, Power Play, Penalty Kill) and what columns to use.
list_str = ['ev', 'pp', 'pk']
columns = ['Player', 'Team', 'Position', 'TOI', 'GP', 'Goals', 'Total Assists', 'Total Points', 'Shots', 'SH%', 'Hits', 'Shots Blocked']
g_columns = ['Player', 'Team', 'GP', 'W', 'L', 'T/O', 'GA', 'SV', 'SV%', 'GAA', 'SO', 'QS%', 'RBS']

# Define the nhl_url and output_path items as blank holders.
nhl_url = ''
output_path = ''

# Normal player stat generator
# Dependancies

import pandas as pd
import datetime
from random import randint
from time import sleep
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta

# Get today's date
today = datetime.now()

# Get the date two weeks ago
two_weeks_ago = today - timedelta(weeks=2)

# Format the dates as strings
today_str = today.strftime("%Y-%m-%d")  # Format as YYYY-MM-DD
two_weeks_ago_str = two_weeks_ago.strftime("%Y-%m-%d")

# Print the results
print("Today's date:", today_str)
print("Date two weeks ago:", two_weeks_ago_str)

# The dates are stored as strings in variables today_str and two_weeks_ago_str

# Define lists for what strengths to use (Even strength, Power Play, Penalty Kill) and what columns to use.
list_str = ['ev', 'pp', 'pk', 'all']
columns = ['Player', 'Team', 'Position', 'TOI', 'GP', 'Goals', 'Total Assists', 'Total Points', 'Shots', 'SH%', 'Hits', 'Shots Blocked']
g_columns = ['Player', 'Team', 'GP', 'W', 'L', 'T/O', 'GA', 'SV', 'SV%', 'GAA', 'SO', 'QS%', 'RBS']
g_tw_columns = ['Name', 'Team', 'GP', 'W', 'L', 'SV', 'SOG', 'GA', 'SO']

# Define the urls and output_path items as blank holders.
nhl_url = ''
output_path = ''

tw_url = ''
output_path_tw = ''

g_url = 'https://www.hockey-reference.com/leagues/NHL_2024_goalies.html'
output_path_g = 'nhl_goalies.csv'

tw_g_url = 'https://www.quanthockey.com/nhl/seasons/last-two-weeks-nhl-goalies-stats.html'
output_path_tw_g = ''

## Goalies
# Goalie two week
output_path_tw_g = f'nhl_tw_goalies.csv'
g_df = pd.read_html(tw_g_url, encoding="ISO-8859-1")[0]
g_dff = pd.DataFrame(g_df, columns=g_tw_columns)
g_dff.to_csv(output_path_tw_g, mode='w', header = True, index = False)
print(output_path_tw_g, "Complete")

# Goalie normal
g_dfs = pd.read_html(g_url)
g_df = g_dfs[0]
g_dff = pd.DataFrame(g_df, columns=g_columns)
g_dff.to_csv(output_path_g, mode='w', header = True, index = False)
print(output_path_g, "Complete")

## Normal skaters stats
for var_str in (list_str):
    # Define the output path, aka the name of the file, using the current strength as the suffix
    output_path = f"nhl_skaters_{var_str}.csv"
    
    # Set the url
    nhl_url = f'https://www.naturalstattrick.com/playerteams.php?fromseason=20242025&thruseason=20242025&stype=2&sit={var_str}&score=all&stdoi=std&rate=n&team=ALL&pos=S&loc=B&toi=0&gpfilt=none&fd=&td=&tgp=410&lines=single&draftteam=ALL'
    
    # Read the url, use the index to grab only the raw chart, define the columns we want the dataframe to use, then export to csv and print that it's done.
    nhl_dfs = pd.read_html(nhl_url)
    nhl_df = nhl_dfs[0]
    nhl_dff = pd.DataFrame(nhl_df, columns=columns)
    nhl_dff.to_csv(output_path, mode='w', header = True, index = False)
    print(output_path, "Complete")
    
    # Sleep function helps us not be detected as a scraper by the website
    sleep(randint(10,25))

## Two week stats

for var_str in (list_str):
    # Set URL and output path using the strength and dates
    tw_url = f'https://www.naturalstattrick.com/playerteams.php?fromseason=20242025&thruseason=20242025&stype=2&sit={var_str}&score=all&stdoi=std&rate=n&team=ALL&pos=S&loc=B&toi=0&gpfilt=gpdate&fd={two_weeks_ago_str}&td={today_str}&tgp=410&lines=single&draftteam=ALL'
    output_path = f"nhl_tw_{var_str}.csv"

    # Read the url, make the dataframe
    tw_dfs = pd.read_html(tw_url)[0]
    tw_dff = pd.DataFrame(tw_dfs, columns=columns)
    tw_dff.to_csv(output_path, mode='w', header = True, index = False)
    print(output_path, "Complete")

    # Sleep function helps us not be detected as a scraper by the website
    sleep(randint(8,20))

print("Complete!")