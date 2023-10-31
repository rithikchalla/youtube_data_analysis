import isodate
import pymongo
import psycopg2
import pandas as pd
import os
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

def get_youtube_api_object():
    return build('youtube', 'v3', developerKey=os.getenv('api_key'))

def get_video_comments(video_id: str, youtube: any) -> list:
    """
        Gets Information about all the comments of a particular video

        Args:
            video_id: string, Video Id of the data that we are trying to retrieve
            youtube: google api build object for interacting with the service
        Returns:
            A list containing dictionary of comments along with when it was published
    """
    comments_data = []
    comments_raw_data = []
    comments = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=50
    ).execute()
    if comments['pageInfo']['totalResults'] == 0:
        return comments_data
    comments_raw_data.extend(comments['items'])
    # while 'nextPageToken' in comments:
    #     comments = youtube.commentThreads().list(
    #         part='snippet',
    #         videoId=videoId,
    #         maxResults=50,
    #         pageToken=comments['nextPageToken']
    #     ).execute()
    #     comments_raw_data.extend(comments['items'])
    for item in comments_raw_data:
        comment = {}
        comment['video_id'] = video_id
        comment['comment_id'] = item['snippet']['topLevelComment']['id']
        comment['comment_text'] = item['snippet']['topLevelComment']['snippet']['textOriginal']
        comment['comment_author'] = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
        comment['comment_published_date'] = item['snippet']['topLevelComment']['snippet']['publishedAt']
        comments_data.append(comment)

    return comments_data

def get_video_information(videoids: list, youtube: any, playlist_id: str) -> list:
    """
        Gets Information about all the video statistics

        Args:
            id: list, Video Ids of the data that we are trying to retrieve
            youtube: google api build object for interacting with the service
            playlist_id: string, playlist id for the current videos
        Returns:
            A list containing dictionary of video statistics along with the comments
    """
    videos_data = []
    videos_raw_data = []
    for id in videoids:
        video_details = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=id
        ).execute()
        if video_details['pageInfo']['totalResults'] == 0:
            continue
        videos_raw_data.extend(video_details['items'])
    for item in videos_raw_data:
        video = {}
        video['playlist_id'] = playlist_id
        video['video_id'] = item['id']
        video['video_name'] = item['snippet']['title']
        video['video_description'] = item['snippet']['description']
        video['video_published_date'] = item['snippet']['publishedAt']
        video['view_count'] = int(item['statistics']['viewCount'])
        video['like_count'] = int(item['statistics']['likeCount']) if 'likeCount' in item['statistics'] else 0
        video['dislike_count'] = int(item['statistics']['dislikeCount']) if 'dislikeCount' in item['statistics'] else 0
        video['favourite_count'] = int(item['statistics']['favoriteCount'])
        video['comment_count'] = int(item['statistics']['commentCount'])
        video['video_duration'] = isodate.parse_duration(item['contentDetails']['duration']).seconds
        video['thumbnail_url'] = item['snippet']['thumbnails']['default']['url']
        video['caption_status'] = item['contentDetails']['caption']
        video['comments'] = get_video_comments(item['id'], youtube)
        videos_data.append(video)

    return videos_data

def get_playlist_videos(playlist_id: str, youtube: any) -> list:
    """
        Gets Information about all the videos associated with a particular playlist

        Args:
            id: string, Playlist Id of a specific youtube channel
            youtube: google api build object for interacting with the service
        Returns:
            A list containing dictionary of playlists along with videos and its statistics
    """
    video_ids = []
    playlist_items_raw_data = []
    channel_playlist_items = youtube.playlistItems().list(
        part = 'snippet',
        playlistId=playlist_id,
        maxResults=50
    ).execute()

    if channel_playlist_items['pageInfo']['totalResults'] == 0:
        return video_ids
    playlist_items_raw_data.extend(channel_playlist_items['items'])
    # while 'nextPageToken' in channel_playlist_items:
    #     channel_playlist_items = youtube.playlistItems().list(
    #         part='snippet',
    #         playlistId=playlist_id,
    #         maxResults=50,
    #         pageToken=channel_playlist_items['nextPageToken']
    #     ).execute()
    #     playlist_items_raw_data.extend(channel_playlist_items['items'])
    for item in playlist_items_raw_data:
        video_ids.append(item['snippet']['resourceId']['videoId'])
    
    return get_video_information(video_ids, youtube, playlist_id)

def get_channel_playlists(id: str, youtube: any) -> list:
    """
        Gets Information about all the playlists of a particular channel

        Args:
            id: string, youtube channel id
            youtube: google api build object for interacting with the service
        Returns:
            A list containing dictionary of playlists along with videos and its statistics
    """
    playlists = []
    playlist_raw_data = []
    channel_playlists = youtube.playlists().list(
        part='snippet',
        channelId=id,
        maxResults=50
    ).execute()

    if channel_playlists['pageInfo']['totalResults'] == 0:
        return playlists

    playlist_raw_data.extend(channel_playlists['items'])
    # while 'nextPageToken' in channel_playlists:
    #     channel_playlists = youtube.playlists().list(
    #         part='snippet',
    #         channelid=id,
    #         maxResults=50,
    #         pageToken=channel_playlists['nextPageToken']
    #     ).execute()
    #     playlist_raw_data.extend(channel_playlists['items'])
    
    for item in playlist_raw_data[:5]:
        playlist_dict = {}
        playlist_dict['playlist_id'] = item['id']
        playlist_dict['channel_id'] = id
        playlist_dict['playlist_name'] = item['snippet']['title']
        playlist_dict['playlist_description'] = item['snippet']['description']
        playlist_dict['videos'] = get_playlist_videos(item['id'], youtube)
        playlists.append(playlist_dict)

    return playlists

def get_youtube_channel_information(id: str, youtube: any) -> dict:
    """
        Gets Information about a channel playlists, videos and comments to the videos

        Args:
            id: string, youtube channel id
            youtube: google api build object for interacting with the service
        Returns:
            A dictionary of channel, its playlists videos and comments associated with them
            Returns empty dictionary if no channel found
    """
    channel_data_dict = {}
    channel_details = youtube.channels().list(
        part='snippet,statistics,status,contentDetails',
        id=id
    ).execute()

    if channel_details['pageInfo']['totalResults'] != 1:
        return channel_data_dict
    
    item = channel_details['items'][0]
    channel_data_dict['channel_id'] = item['id']
    channel_data_dict['channel_name'] = item['snippet']['title']
    channel_data_dict['channel_views'] = int(item['statistics']['viewCount'])
    channel_data_dict['channel_description'] = item['snippet']['description']
    channel_data_dict['status'] = item['status']['privacyStatus']
    channel_data_dict['channel_subscribers'] = int(item['statistics']['subscriberCount'])
    channel_data_dict['channel_video_count'] = int(item['statistics']['videoCount'])
    channel_data_dict['playlists'] = get_channel_playlists(item['id'], youtube)

    return channel_data_dict
    
def save_data_to_mongo_db(data,mongo_collection):

    #First delete the existing data from the mongo
    result = mongo_collection.delete_one({"channel_id": data['channel_id']})

    # Insert the data into mongo
    result = mongo_collection.insert_one(data)

def get_channel_names_datalake(query, mongo_collection):
    cursor = mongo_collection.find(query)
    channel_names = []
    for document in cursor:
        channel_names.append(document['channel_name'])
    return channel_names

def get_channel_details_datalake(query, mongo_collection):

    cursor = mongo_collection.find(query)
    for document in cursor:
        return document
    return []

def connect_to_postgre():
    """
        Connects to a PostgreSQL database running on GCP.
        Args:
            host: The host of the PostgreSQL database instance.
            port: The port of the PostgreSQL database instance.
            database: The name of the PostgreSQL database.
            username: The username to use to connect to the PostgreSQL database.
            password: The password to use to connect to the PostgreSQL database.
        Returns:
            A psycopg2 connection object.
    """

    return psycopg2.connect(
        host=os.getenv('postgre_host'),
        port=5432,
        database='youtube_data',
        user='postgres',
        password=os.getenv('postgres_db_password')
    )
    
def insert_data_into_postgre(data, connection):
    """
        Inserts data into a postgre running on GCP.
        Args:
            data: dictionary of the data
            connection: psycopg2 connection object
        Returns:
            A psycopg2 connection object.
    """
    cursor = connection.cursor()

    delete_query = """
                        DELETE FROM myschema."Comment"
                        WHERE video_id IN (
                            SELECT a.video_id FROM myschema."Video" a
                            INNER JOIN myschema.playlist b on a.playlist_id = b.playlist_id
                                AND b.channel_id = '{}'
                        );

                        DELETE FROM myschema."Video"
                        WHERE playlist_id IN (
                            select playlist_id FROM myschema.playlist WHERE channel_id = '{}'
                        );

                        DELETE FROM myschema."playlist"
                        WHERE channel_id = '{}';

                        DELETE FROM myschema."channel_details" WHERE channel_id = '{}';
                    """.format(data['channel_id'], data['channel_id'], data['channel_id'], data['channel_id'])
    
    cursor.execute(delete_query)

    insert_query = """
                        INSERT INTO myschema."channel_details"
                        (channel_id, channel_name, channel_views, 
                        channel_description, channel_status, channel_subscribers, channel_video_count)
                        VALUES
                        (
                            %(channel_id)s, %(channel_name)s, %(channel_views)s, %(channel_description)s,
                            %(status)s, %(channel_subscribers)s, %(channel_video_count)s
                        )
                    """
    channel_data = [
        {
            'channel_id': data['channel_id'],
            'channel_name': data['channel_name'],
            'channel_views': data['channel_views'],
            'channel_description': data['channel_description'],
            'status': data['status'],
            'channel_subscribers': data['channel_subscribers'],
            'channel_video_count': data['channel_video_count'],
        }
    ]
    cursor.executemany(insert_query, channel_data)

    insert_query_playlist = """
                        INSERT INTO myschema."playlist"
                        (playlist_id, channel_id, playlist_name, playlist_description)
                        VALUES
                        (
                            %(playlist_id)s, %(channel_id)s, %(playlist_name)s, %(playlist_description)s
                        )
                    """

    cursor.executemany(insert_query_playlist, data['playlists'])

    video_data = []
    for playlist in data['playlists']:
        for video in playlist['videos']:
            if not any(d['video_id'] == video['video_id'] for d in video_data):
                video_data.append(video)

    insert_query_video = """
                            INSERT INTO myschema."Video"(
	                        video_id, playlist_id, video_name, video_description, 
                            published_date, view_count, like_count, dislike_count, 
                            favourite_count, comment_count, duration, thumbnail_url, 
                            caption_status)
                            VALUES
                            (
                                %(video_id)s, %(playlist_id)s, %(video_name)s, %(video_description)s,
                                %(video_published_date)s, %(view_count)s, %(like_count)s, 
                                %(dislike_count)s, %(favourite_count)s, %(comment_count)s,
                                %(video_duration)s, %(thumbnail_url)s, %(caption_status)s
                            )
                        """
    cursor.executemany(insert_query_video, video_data)

    comment_data = []
    for video in video_data:
        comment_data.extend(video['comments'][:2])
    insert_query_comments = """
                                INSERT INTO myschema."Comment"(
	                            comment_id, video_id, comment_text, comment_author, 
                                comment_published_date)
                                VALUES
                                (
                                    %(comment_id)s, %(video_id)s, %(comment_text)s,
                                    %(comment_author)s, %(comment_published_date)s
                                )
                            """
    cursor.executemany(insert_query_comments, comment_data)
    connection.commit()

    cursor.close()

def get_mongo_client():
    client = pymongo.MongoClient(os.getenv('mongo_uri'))
    mongo_db = client["youtube_import"]
    return mongo_db["channels_data"]

def get_sql_query_results(query, connection):
    try:
        cursor = connection.cursor()
        cursor.execute(query)

        return cursor.fetchall()
    finally:
        cursor.close()