# Introduction
With this application user has the ability to input a youtube channel id or set of comma seperated
channel id and get information related to channels such as title, description, playlists, videos, likes, dislikes, counts e.t.c. Once the user inputs a channelid and if the channel id is found the data gets automatically saved into a mongo datalake. User also has the ability to select a channelid or select multiple and save them into a Postgre SQL Data warehouse for further analysis.

# How to run
1. First download all the libraries mentioned in the requirements.txt file
2. create a .env file with the below structure</b>
```
api_key = '<your api key>'
mongo_uri = '<mongo db connection uri>'
postgre_host = '<postgre_host>'
postgres_db_password = '<postgre_db_password>'
```
3. Run the streamlit app using the command</b>
```streamlit run streamlit_ui.py```