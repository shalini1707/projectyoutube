import streamlit as st
from googleapiclient.discovery import build
import pymongo
import json
from bson import ObjectId
import pandas as pd
from sqlalchemy import create_engine,inspect
from sqlalchemy import text
import pymysql


# Connect to MongoDB Atlas
mongo_client = pymongo.MongoClient("mongodb+srv://shalini:shalini17@cluster0.am8nejz.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_client["data"]
channel_collection = mongo_db["channel"]
playlist_collection = mongo_db["playlist"]
comment_collection = mongo_db["comment"]
video_collection = mongo_db["video"]



mysql_host = "localhost"
mysql_user = "root"
mysql_password = "12345"
mysql_database = "data"
mysql_table_channel = "channel_table"
mysql_table_playlist = "playlist_table"
mysql_table_comment = "comment_table"
mysql_table_video = "video_table"


api_key = "AIzaSyAY9gbbkfqoqwkOBr8R5PibSr3OTNwur_0"

engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}")

st.title("youtube scraping")

selected_channel_id = None


def retrieve_channel_data(channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    response = youtube.channels().list(part="snippet,statistics", id=channel_id).execute()
    items = response.get("items", [])
    if items:
        item = items[0]
        channel_name = item["snippet"]["title"]
        channel_views = item["statistics"]["viewCount"]
        channel_description = item["snippet"]["description"]
        channel_type = item["snippet"].get("channelType", "Unknown")
        channel_status = item["snippet"].get("status", {}).get("privacyStatus", "Unknown")
        return {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "channel_type": channel_type,
            "channel_views": channel_views,
            "channel_description": channel_description,
            "channel_status": channel_status
        }
    else:
        st.error("Channel not found!")
        return None


def retrieve_playlist_data(channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    response = youtube.playlists().list(part="snippet", channelId=channel_id, maxResults=50).execute()
    playlists = response.get("items", [])
    if playlists:
        playlist_data = []
        for playlist in playlists:
            playlist_id = playlist["id"]
            playlist_name = playlist["snippet"]["title"]
            playlist_data.append({
                "playlist_id": playlist_id,
                "channel_id": channel_id,
                "playlist_name": playlist_name
            })
        return playlist_data
    else:
        st.warning("No playlists found for the channel!")
        return None
    


def retrieve_comment_data(channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    response = youtube.commentThreads().list(part="snippet", allThreadsRelatedToChannelId=channel_id, maxResults=50).execute()
    comments = response.get("items", [])
    if comments:
        comment_data = []
        for comment in comments:
            comment_id = comment["id"]
            video_id = comment["snippet"]["videoId"]
            comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comment_author = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            comment_data.append({
                "comment_id": comment_id,
                "video_id": video_id,
                "comment_text": comment_text,
                "comment_author": comment_author
            })
        return comment_data
    else:
        st.warning("No comments found for the channel!")
        return None



def retrieve_video_data(channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    playlists = youtube.playlists().list(part="snippet", channelId=channel_id, maxResults=50).execute()
    playlist_items = playlists.get("items", [])

    video_data = []
    for playlist_item in playlist_items:
        playlist_id = playlist_item["id"]
        playlist_title = playlist_item["snippet"]["title"]

        playlist_videos = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlist_id, maxResults=50).execute()
        videos = playlist_videos.get("items", [])

        for video in videos:
            video_id = video["snippet"]["resourceId"]["videoId"]
            video_title = video["snippet"]["title"]
            video_published_at = video["snippet"]["publishedAt"]
            video_view_count = video.get("statistics", {}).get("viewCount", 0)
            video_like_count = video.get("statistics", {}).get("likeCount", 0)
            video_dislike_count = video.get("statistics", {}).get("dislikeCount", 0)
            video_favorite_count = video.get("statistics", {}).get("favoriteCount", 0)
            video_comment_count = video.get("statistics", {}).get("commentCount", 0)

            video_data.append({
                "video_id": video_id,
                "playlist_id": playlist_id,
                "channel_id": channel_id,
                "playlist_title": playlist_title,
                "video_title": video_title,
                "video_published_at": video_published_at,
                "video_view_count": video_view_count,
                "video_like_count": video_like_count,
                "video_dislike_count": video_dislike_count,
                "video_favorite_count": video_favorite_count,
                "video_comment_count": video_comment_count,
                "video_duration": ""
            })

    return video_data

def store_channel_data(channel_id):
    channel_data = retrieve_channel_data(channel_id)
    if channel_data:
        channel_collection.insert_one(channel_data)

def store_playlist_data(channel_id):
    playlist_data = retrieve_playlist_data(channel_id)
    if playlist_data:
        playlist_collection.insert_many(playlist_data)

def store_comment_data(channel_id):
    comment_data = retrieve_comment_data(channel_id)
    if comment_data:
        comment_collection.insert_many(comment_data)

def store_video_data(channel_id):
    video_data = retrieve_video_data(channel_id)
    if video_data:
        video_collection.insert_many(video_data)



stored_channel_ids = set()

stored_channel_ids = channel_collection.distinct("channel_id")

def store_data(channel_id):
    if channel_id in stored_channel_ids:
        return

    store_channel_data(channel_id)
    store_playlist_data(channel_id)
    store_comment_data(channel_id)
    store_video_data(channel_id)
    stored_channel_ids.add(channel_id)

def cselector():

    channel_ids = [
        "UC-w2vyX6uMb8k4AZwnQN_MA",  # amma
        "UC_exm4hbJ2zV-4SrqrL9IRw",
        "UC7RZIGGgAp8P4z354boxGtg",
        "UCHGktfcQq2BY_8tGPHwvm7g",
        "UCNiW2gFbOFy9A7_SW6MxdYQ",
        "UC7fQFl37yAOaPaoxQm-TqSA",
        "UCSUafMuEUUe3Ap4_0HD8l0w",
        "UCeUu1y9XD6QQbkKcStLPvXw",
        "UCduIoIMfD8tT3KoU0-zBRgQ",
        "UCZM82IdwJeS1As0lPZu5_Xg"  # ven
    ]
    selected_channel = st.selectbox("Select a channel", channel_ids)
    st.write("Selected channel:", selected_channel)
    return selected_channel

def monjson(selected_channel):
    channel_data = channel_collection.find_one({"channel_id": selected_channel})
    playlist_data = list(playlist_collection.find({"channel_id": selected_channel}))
    comment_data = retrieve_comment_data(selected_channel)
    video_data = list(video_collection.find({"channel_id": selected_channel}))

    st.title("Channel")
    st.json(channel_data)
    channel_df = pd.DataFrame([channel_data])
    channel_df

    st.title("Playlists")
    st.json(playlist_data)
    playlist_df = pd.DataFrame(playlist_data)
    playlist_df

    st.title("Comment")
    st.json(comment_data)
    comment_df = pd.DataFrame(comment_data)
    comment_df

    st.title("Video")
    st.json(video_data)
    video_df = pd.DataFrame(video_data)
    video_df

def retrieve_data_from_mongodb():
    # Retrieve data from MongoDB collections and convert to DataFrames
    channel_data = list(channel_collection.find())
    playlist_data = list(playlist_collection.find())
    comment_data = list(comment_collection.find())
    video_data = list(video_collection.find())

    channel_df = pd.DataFrame(channel_data)
    playlist_df = pd.DataFrame(playlist_data)
    comment_df = pd.DataFrame(comment_data)
    video_df = pd.DataFrame(video_data)

    return channel_df, playlist_df, comment_df, video_df

def migrate_data_to_mysql():
    channel_df, playlist_df, comment_df, video_df = retrieve_data_from_mongodb()

    # Check if migration has already been done
    inspector = inspect(engine)
    if not inspector.has_table('channel_table'):
        # Migrate DataFrames to MySQL tables
        channel_df.to_sql(name='channel_table', con=engine, if_exists='append', index=False)
        playlist_df.to_sql(name='playlist_table', con=engine, if_exists='append', index=False)
        comment_df.to_sql(name='comment_table', con=engine, if_exists='append', index=False)
        video_df.to_sql(name='video_table', con=engine, if_exists='append', index=False)
        st.write("Data migration completed successfully.")
    else:
        #st.write("Data migration has already been done. Skipping migration.")
        pass

def migrate():
    migrate_data_to_mysql()

if __name__ == '__main__':
    selected_channel = cselector()
    if selected_channel is not None:
        store_data(selected_channel)
        monjson(selected_channel)

        
        


# Query 1: Names of all videos and their corresponding channels
query1= "SELECT video_table.video_title, channel_table.channel_name FROM video_table INNER JOIN channel_table ON video_table.channel_id = channel_table.channel_id"
df1 = pd.read_sql(query1, engine)


query2 = "SELECT channel_table.channel_name, COUNT(video_table.video_id) AS num_videos FROM channel_table INNER JOIN video_table ON channel_table.channel_id = video_table.channel_id GROUP BY channel_table.channel_name ORDER BY num_videos DESC"
df2 = pd.read_sql(query2, engine)



# Query: Number of Comments per Video
query3 = "SELECT video_table.video_title, COUNT(comment_table.comment_id) AS num_comments FROM video_table LEFT JOIN comment_table ON video_table.video_id = comment_table.video_id GROUP BY video_table.video_title"
df3 = pd.read_sql(query3, engine)



# Query: Videos with Highest Number of Comments
query4= """
SELECT video_table.video_title, channel_table.channel_name, COUNT(comment_table.comment_id) AS num_comments
FROM video_table
INNER JOIN channel_table ON video_table.channel_id = channel_table.channel_id
LEFT JOIN comment_table ON video_table.video_id = comment_table.video_id
GROUP BY video_table.video_title, channel_table.channel_name
ORDER BY num_comments DESC
"""
df4 = pd.read_sql(query4, engine)


# Query: Number of Comments per Video
query5= """
SELECT video_table.video_title, COUNT(comment_table.comment_id) AS num_comments
FROM video_table
LEFT JOIN comment_table ON video_table.video_id = comment_table.video_id
GROUP BY video_table.video_title
"""
df5 = pd.read_sql(query5, engine)






# Create dropdown lists for query selection
query_options = {
        "Video and Channel Names": df1,
         "Channels with Most Videos": df2,
          "Number of Comments per Video": df3,
            "highest number of comments": df4,
            "Videos with Highest Number of Comments and Corresponding Channel Names": df5
             
          
        
}
# Streamlit app code
st.title("MySQL Query Results")
selected_query = st.selectbox("Select a query", list(query_options.keys()))

if selected_query:
        st.header(selected_query)
        df = query_options[selected_query]
        st.table(df)



 

mongo_client.close()
engine.dispose()
