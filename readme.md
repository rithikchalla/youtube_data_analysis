# YouTube Data Analysis with Streamlit

This Python script provides a Streamlit web application for analyzing YouTube data. The script uses the Streamlit library to create a user interface, fetches data from the YouTube Data API, stores it in MongoDB, and performs various data analyses using PostgreSQL.

## Prerequisites
Before running the script, make sure you have the following:

1. Python installed on your machine
2. Required Python packages installed. You can install them using: 
```
pip install -r requirements.txt
```
3. Set up a .env file in the same directory as the script with the following content:
```
api_key=YOUR_YOUTUBE_API_KEY
mongo_uri=YOUR_MONGO_DB_URI
postgre_host=YOUR_POSTGRE_HOST
postgre_db_password=YOUR_POSTGRE_DB_PASSWORD
```

## How to Run
1. Open a terminal and navigate to the directory containing the script.
2. Run the Streamlit app with the following command:
```
streamlit run streamlit_ui.py
```

## Features

### 1. GetData
- Enter YouTube channel IDs (comma-separated) in the sidebar.
- Click the "Search" button to fetch information about the specified channels.
- Data is saved to MongoDB.

### 2. Migrate Data to Warehouse
- Select channels from the ones available in the data lake for migration.
- Click the "Migrate to Data Warehouse" button to move data to PostgreSQL.

### 3. Analyze Data
Three data analysis sections:

1. **Video and Channel Names**
2. **Channels with Most Videos**
3. **Top 10 Most Viewed Videos**

Each section has an expander for detailed analysis with SQL queries.

## Data Storage

- MongoDB is used for storing raw channel data.
- PostgreSQL is used for data warehousing.

## Configuration

### YouTube Data API Key

Make sure to replace `YOUR_YOUTUBE_API_KEY` in the `.env` file with a valid YouTube Data API key.

### MongoDB and PostgreSQL Connection

Update the MongoDB URI and PostgreSQL connection details in the `.env` file.

## Issues and Contributions

If you encounter any issues or would like to contribute, feel free to open an issue or create a pull request.

Happy coding!