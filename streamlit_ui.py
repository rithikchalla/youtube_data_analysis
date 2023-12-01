import streamlit as st
import streamlit_api as yt
import pandas as pd

if 'mongo_collection' not in st.session_state:
    st.session_state.mongo_collection = yt.get_mongo_client()

if 'sql_client' not in st.session_state:
    st.session_state.sql_client = yt.connect_to_postgre()

selection = st.sidebar.selectbox(
    "Select an option from the below options",
    ("GetData", "Migrate Dara to Warehouse", "Analyze data")
)

st.title("Youtube Data Analysis")

if selection == 'GetData':
    channel_ids = st.sidebar.text_input("Please Enter a channel Id or comma seperated Ids")

    if st.sidebar.button('Search'):
        try:
            with st.spinner('searching.....'):
                channel_info = []
                channel_display_data = []
                for id in channel_ids.split(','):
                    data = yt.get_youtube_channel_information(id, yt.get_youtube_api_object())
                    if data != {}:
                        channel_info.append(data)
                        channel_display_data.append({
                            'channel_id': data['channel_id'],
                            'channel_name': data['channel_name'],
                            'channel_description': data['channel_description'],
                            'channel_subscribers': data['channel_subscribers'],
                            'channel_video_count': data['channel_video_count']
                        })
                if len(channel_info) > 0:
                    for channel in channel_info:
                        yt.save_data_to_mongo_db(channel, st.session_state.mongo_collection)
                    st.success('successfully found the channel and saved to data lake')
                    st.write(channel_display_data)
                else:
                    st.info('Unable to find the channel please try again')
        except Exception as e:
            st.write(e)

elif selection == 'Migrate Dara to Warehouse':
    channels_to_migrate = yt.get_channel_names_datalake({}, st.session_state.mongo_collection)

    selected_channels_for_migration = st.sidebar.multiselect('Select channels you want to migrate to data warehouse', channels_to_migrate)

    if st.sidebar.button('Migrate to Data Warehouse'):
        try:
            with st.spinner('Migrating Data to warehouse........'):
                for channel in selected_channels_for_migration:
                    channel_info_lake = yt.get_channel_details_datalake({'channel_name': channel}, st.session_state.mongo_collection)
                    yt.insert_data_into_postgre(channel_info_lake, st.session_state.sql_client )
                st.success('Successfully migrated the below channels to data warehouse')
                st.write(selected_channels_for_migration)
        except Exception as e:
            st.write(e)

elif selection == 'Analyze data':
    with st.expander('What are the names of all videos and their corresponding channels?'):
        query = '''
                    SELECT a.video_name, c.channel_name 
                    FROM myschema."Video" a
                    INNER JOIN myschema."playlist" b 
                        ON a.playlist_id = b.playlist_id
                    INNER JOIN myschema."channel_details" c
                        ON b.channel_id = c.channel_id;
                '''
        df_results = pd.DataFrame(yt.get_sql_query_results(query, st.session_state.sql_client), 
                                  columns=('Video Name', 'Channel Name'))
        st.table(df_results)

    with st.expander('Which channels have the most number of videos and how many videos do they have?'):
        query1 = '''
                    SELECT channel_name, channel_video_count 
                    FROM myschema."channel_details"
                    ORDER BY channel_video_count desc
                    LIMIT 2;
                '''
        df_results1 = pd.DataFrame(yt.get_sql_query_results(query1, st.session_state.sql_client), 
                                   columns=('Channel Name', 'Total Video Count'))
        st.table(df_results1)
    with st.expander('What are the top 10 most viewed videos and their respective channels?'):
        query2 = '''
                    SELECT a.video_name, a.view_count, c.channel_name
                    FROM myschema."Video" a
                    INNER JOIN myschema."playlist" b 
                        ON a.playlist_id = b.playlist_id
                    INNER JOIN myschema."channel_details" c
                        ON b.channel_id = c.channel_id
                    ORDER BY a.view_count DESC
                    LIMIT 10;
                '''
        df_results2 = pd.DataFrame(yt.get_sql_query_results(query2, st.session_state.sql_client), 
                                   columns=('Video Title', 'Total Views', 'Channel Name'))
        st.table(df_results2)
    with st.expander('How many comments were made on each video, and what are their corresponding video names?'):
        query3 = '''
                    SELECT video_name, comment_count
                    FROM myschema."Video"
                    ORDER BY comment_count desc;
                '''
        df_results3 = pd.DataFrame(yt.get_sql_query_results(query3, st.session_state.sql_client), 
                                   columns=('Video Title', 'Total comments'))
        st.table(df_results3)
    with st.expander('Which videos have the highest number of likes and what are their corresponding channel names?'):
        query4 = '''
                    SELECT a.video_name, a.like_count, c.channel_name
                    FROM myschema."Video" a
                    INNER JOIN myschema."playlist" b 
                        ON a.playlist_id = b.playlist_id
                    INNER JOIN myschema."channel_details" c
                        ON b.channel_id = c.channel_id
                    ORDER BY a.like_count DESC
                    LIMIT 100;
                '''
        df_results4 = pd.DataFrame(yt.get_sql_query_results(query4, st.session_state.sql_client), 
                                   columns=('Video Title', 'Total Likes', 'Channel Name'))
        st.table(df_results4)
    with st.expander('What is the total number of likes and dislikes for each video, and what are their corresponding video names?'):
        query5 = '''
                    SELECT video_name, like_count, dislike_count
                    FROM myschema."Video";
                '''
        df_results5 = pd.DataFrame(yt.get_sql_query_results(query5, st.session_state.sql_client), 
                                   columns=('Video Title', 'Total Likes', 'Total dislikes'))
        st.table(df_results5)
    with st.expander('What are the total number of views for each channel and what are their corresponding channel names'):
        query6 = '''
                    SELECT channel_name, channel_views
                    FROM myschema."channel_details";
                '''
        df_results6 = pd.DataFrame(yt.get_sql_query_results(query6, st.session_state.sql_client), 
                                   columns=('Channel name', 'Total Views'))
        st.table(df_results6)
    with st.expander('What are the names of all the channels that have published videos in the year 2022?'):
        query7 = '''
                    SELECT DISTINCT c.channel_name 
                    FROM myschema."Video" a
                    INNER JOIN myschema."playlist" b 
                        ON a.playlist_id = b.playlist_id
                    INNER JOIN myschema."channel_details" c
                        ON b.channel_id = c.channel_id
                    WHERE a.published_date BETWEEN ('2022-01-01') AND ('2022-12-31');
                '''
        df_results7 = pd.DataFrame(yt.get_sql_query_results(query7, st.session_state.sql_client), 
                                   columns=['Channel name'])
        st.table(df_results7)
    with st.expander('What is the average duration of all videos in each channel and what are their corresponding channel names'):
        query8 = '''
                    SELECT c.channel_name, AVG(a.duration)/60 AS average_duration
                    FROM myschema."Video" a
                    INNER JOIN myschema."playlist" b 
                        ON a.playlist_id = b.playlist_id
                    INNER JOIN myschema."channel_details" c
                        ON b.channel_id = c.channel_id
                    GROUP BY c.channel_name;
                '''
        df_results8 = pd.DataFrame(yt.get_sql_query_results(query8, st.session_state.sql_client), 
                                   columns=('Channel name', 'Average Duration in mins'))
        st.table(df_results8)
    with st.expander('Which videos have the highest number of comments, and what are their corresponding channel names?'):
        query9 = '''
                    SELECT a.video_name, c.channel_name, a.comment_count
                    FROM myschema."Video" a
                    INNER JOIN myschema."playlist" b 
                        ON a.playlist_id = b.playlist_id
                    INNER JOIN myschema."channel_details" c
                        ON b.channel_id = c.channel_id
                    ORDER BY a.comment_count desc
                    LIMIT 100;
                '''
        df_results9 = pd.DataFrame(yt.get_sql_query_results(query9, st.session_state.sql_client), 
                                   columns=('Video Title', 'Channel Name', 'Comment Count'))
        st.table(df_results9)