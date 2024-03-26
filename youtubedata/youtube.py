
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
from datetime import datetime
import pymongo
from googleapiclient.errors import HttpError
import streamlit as st
api_key = "AIzaSyB91Cky4mnMRIaEaqcWMLnJCY9I9h-ZTTI"
mongodb_client=pymongo.MongoClient("mongodb+srv://kabila:charujaisty@kabila.dnfbuya.mongodb.net/?retryWrites=true&w=majority&appName=kabila")
db=mongodb_client["youtube"]
col=db["Channel_Details"]
def api_connect():
 api_id="AIzaSyB91Cky4mnMRIaEaqcWMLnJCY9I9h-ZTTI"
 api_service_name="youtube"
 api_version="v3"
 youtube=build(api_service_name, api_version,developerKey=api_id)

 return youtube
youtube=api_connect()
#sql connection
mydb = psycopg2.connect(
                        user = 'postgres',
                        password = 'kabilajaisty',
                        host = 'localhost',
                        database = 'youtube',
                        port = 5432)
mycursor = mydb.cursor()
# Channel details

def Channel_info(Channel_Id):

  request=youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=Channel_Id
  )
  response=request.execute()
  for i in response["items"]:
    data=dict(Channel_Name=i["snippet"]["title"],
              Channel_Id=i["id"],
              Subscribers=i["statistics"]["subscriberCount"],
              viewCount=i["statistics"]["viewCount"],
              VideoCount=i["statistics"]["videoCount"],
              description=i["snippet"]["description"],
              Playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
  
#video ids
def Video_ids(Channel_Id):
    video_ids = []
    response = youtube.channels().list(
        id=Channel_Id,
        part="snippet,contentDetails,statistics"
    ).execute()
    playlist_id = response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None :
            break

    return video_ids

# video informations

def video_info(video_ids):
  video_data=[]
  for video_id in video_ids:
    request=youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id  
    )
    response=request.execute()
    for i in response["items"]:
      data=dict(Channel_Name=i["snippet"]["channelTitle"],
                Channel_Id=i["snippet"]["channelId"],
                VideoId=i["id"],
                Tags=i.get("etag"),
                Video_Description=i["snippet"]["description"],
                PublishedAt=i["snippet"]["publishedAt"],
                View_Count=i["statistics"]["viewCount"],
                Like_Count=i["statistics"].get("likes"),
                Comment_Count=i["statistics"].get("commentCount"),
                Duration=i["contentDetails"].get("duration"),
                Caption_Status=i.get("caption"),
                Favorite_counts=i["statistics"]["favoriteCount"])
      video_data.append(data)
  return video_data
  
# get comment information
def Comment_info(video_ids):

  comments_info_list = []

  for Video_id in video_ids:
        try:
          request = youtube.commentThreads().list(
                                                  part = "snippet,replies",
                                                  maxResults = 10,
                                                  videoId = Video_id
                                                  )
          response = request.execute()

          
          for item  in response.get('items'):
            top_level_comment = item['snippet']['topLevelComment']['snippet']
            if top_level_comment:
                      comment_data = {
                          'video_id': top_level_comment.get('videoId'),
                          'comment_text': top_level_comment.get('textOriginal'),
                          'comment_author': top_level_comment.get('authorDisplayName'),
                          'comment_publishedAt': top_level_comment.get('publishedAt')
                      }
                      comments_info_list.append(comment_data)
        except HttpError as e:
            if e.resp.status == 403:
                print(f"Comments are disabled for video ID {Video_id}.")
            else:
                print(f"An error occurred while fetching comments for video ID {Video_id}: {e}")
  
  return comments_info_list

def Playlist_info(Channel_Id):
  All_data=[]
  request=youtube.playlists().list(
      part="snippet,contentDetails",
      channelId=Channel_Id

  )
  response=request.execute()

  for i in response["items"]:
    data=dict(Playlist_Id=i["id"],
              Title=i["snippet"]["title"],
              channelId=i["snippet"]["channelId"],
              channelTitle=i["snippet"]["channelTitle"],
              publishedAt=i["snippet"]["publishedAt"])
    All_data.append(data)

  return All_data

def Channel_Details(Channel_Id):
  cha_details=Channel_info(Channel_Id)
  play_details=Playlist_info(Channel_Id)
  Video_id=Video_ids(Channel_Id)
  vid_deatils=video_info(Video_id)
  com_details=Comment_info(Video_id)

  coll=db["Channel_Details"]
  coll.insert_one({"channel_information":cha_details,"playlist_information":play_details,
                   "video_information":vid_deatils,"comment_information":com_details})

  return "upload successful"
#Mongodb to sql

def channel_table():
    mydb = psycopg2.connect(
                        user = 'postgres',
                        password = 'kabilajaisty',
                        host = 'localhost',
                        database = 'youtube',
                        port = 5432)
    mycursor = mydb.cursor()
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Subscribers bigint,
                                                        viewCount bigint,
                                                        VideoCount int,
                                                        description text,
                                                        Playlist_id varchar(100) )'''
    mycursor.execute(create_query)
    mydb.commit()


    channel_list=[]
    db=mongodb_client["youtube"]
    col=db["Channel_Details"]
    for cha_data in col.find({},{"_id":0,"channel_information":1}):
        channel_list.append(cha_data["channel_information"])
    df=pd.DataFrame(channel_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            viewCount,
                                            VideoCount,
                                            description,
                                            Playlist_id )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row["Channel_Name"],
            row["Channel_Id"],
            row["Subscribers"],
            row["viewCount"],
            row["VideoCount"],
            row["description"],
            row["Playlist_id"])

    mycursor.execute(insert_query,values)
    mydb.commit()


def playlist_table():
# Establish connection to PostgreSQL
    mydb = psycopg2.connect(
        user='postgres',
        password='kabilajaisty',
        host='localhost',
        database='youtube',
        port=5432)
    mycursor = mydb.cursor()

    # Create table if not exists
    create_query = '''create table if not exists playlists(
                        Playlist_Id varchar(100),
                        Title varchar(100),
                        channelId varchar(100),
                        channelTitle varchar(100),
                        publishedAt varchar(100))'''  
    mycursor.execute(create_query)
    mydb.commit()
    # Alter table to change data type of publishedAt column to VARCHAR
    alter_query = "ALTER TABLE playlists ALTER COLUMN publishedAt TYPE VARCHAR(100)"
    mycursor.execute(alter_query)
    mydb.commit()

    # Fetch data from MongoDB
    playlist_list = []
    db = mongodb_client["youtube"]
    col = db["Channel_Details"]

    for ply_data in col.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(ply_data["playlist_information"])):
            playlist_list.append(ply_data["playlist_information"][i])

    df2 = pd.DataFrame(playlist_list)


    for index, row in df2.iterrows():
        insert_query = '''insert into playlists(
                            Playlist_Id,
                            Title,
                            channelId,
                            channelTitle,
                            publishedAt)
                            values(%s, %s, %s, %s, %s)'''

        values = (row["Playlist_Id"],
                row["Title"],
                row["channelId"],
                row["channelTitle"],
                row["publishedAt"]) 

        mycursor.execute(insert_query, values)
        mydb.commit()



#videos table
def videos_table():
    mydb = psycopg2.connect(
        user='postgres',
        password='kabilajaisty',
        host='localhost',
        database='youtube',
        port=5432)
    mycursor = mydb.cursor()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    VideoId varchar(100),
                                                    Tags text,
                                                    Video_Description text,
                                                    PublishedAt varchar(100),
                                                    View_Count bigint,
                                                    Like_Count bigint,
                                                    Comment_Count int,
                                                    Duration interval,
                                                    Caption_Status varchar(100),
                                                    Favorite_counts int)'''
    mycursor.execute(create_query)
    mydb.commit()
    # Alter table to change data type of publishedAt column to VARCHAR
    alter_query = "ALTER TABLE videos ALTER COLUMN publishedAt TYPE VARCHAR(100)"
    mycursor.execute(alter_query)
    mydb.commit()

    
    videos_list= []
    db = mongodb_client["youtube"]
    col = db["Channel_Details"]

    for vid_data in col.find({}, {"_id": 0, "video_information":1}):
        for i in range(len(vid_data["video_information"])):
            videos_list.append(vid_data["video_information"][i])
    df3 = pd.DataFrame(videos_list)
    for index,row in df3.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                        Channel_Id,
                                        VideoId,
                                        Tags,
                                        Video_Description,
                                        PublishedAt,
                                        View_Count,
                                        Like_Count,
                                        Comment_Count,
                                        Duration,
                                        Caption_Status,
                                        Favorite_counts)
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row["Channel_Name"],
                row["Channel_Id"],
                row["VideoId"],
                row["Tags"],
                row["Video_Description"],
                row["PublishedAt"],
                row["View_Count"],
                row["Like_Count"],
                row["Comment_Count"],
                row["Duration"],
                row["Caption_Status"],
                row["Favorite_counts"])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

def comments_table():
    mydb = psycopg2.connect(
        user='postgres',
        password='kabilajaisty',
        host='localhost',
        database='youtube',
        port=5432)
    mycursor = mydb.cursor()

    create_query='''create table if not exists comments(video_id text,
                                                        comment_text text,
                                                        comment_author varchar(100),
                                                        comment_publishedAt timestamp
                                                        )'''


    mycursor.execute(create_query)
    mydb.commit()


    comment_list= []
    db = mongodb_client["youtube"]
    col = db["Channel_Details"]
    for com_data in col.find({}, {"_id": 0, "comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])
    df4 = pd.DataFrame(comment_list)


    for index,row in df4.iterrows():
        insert_query='''insert into comments(video_id,
                                        comment_text,
                                        comment_author,
                                        comment_publishedAt)
                                        values(%s,%s,%s,%s)'''
        
        values=(row["video_id"],
                row["comment_text"],
                row["comment_author"],
                row["comment_publishedAt"])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

def Tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Table created"

def show_channel_tables():
    channel_list=[]
    db=mongodb_client["youtube"]
    col=db["Channel_Details"]
    for cha_data in col.find({},{"_id":0,"channel_information":1}):
        channel_list.append(cha_data["channel_information"])
    df=st.dataframe(channel_list)

    return df

def show_playlist_tables():
    playlist_list = []
    db = mongodb_client["youtube"]
    col = db["Channel_Details"]
    for ply_data in col.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(ply_data["playlist_information"])):
            playlist_list.append(ply_data["playlist_information"][i])
    df2 = st.dataframe(playlist_list)

    return df2

def show_video_tables():
    videos_list= []
    db = mongodb_client["youtube"]
    col = db["Channel_Details"]
    for vid_data in col.find({}, {"_id": 0, "video_information":1}):
        for i in range(len(vid_data["video_information"])):
            videos_list.append(vid_data["video_information"][i])
    df3 = st.dataframe(videos_list)

    return df3

def show_comment_tables():
    comment_list= []
    db = mongodb_client["youtube"]
    col = db["Channel_Details"]
    for com_data in col.find({}, {"_id": 0, "comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])
    df4 = st.dataframe(comment_list)

    return df4




    
# Using streamlit for providing an interactive interface to query and analyze the collected data.
st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")

with st.sidebar:
    st.title(":blue[SCARP]")
    channel_id=st.text_input("ENTER CHANNEL ID")
    if st.button("Collect Data"):
        cha_ids=[]
        db=mongodb_client["youtube"]
        col=db["Channel_Details"]
        for ch_data in col.find({},{"_id":0,"channel_information":1}):
            cha_ids.append(ch_data["channel_information"]["Channel_Id"])

            if channel_id in cha_ids:
                st.success("Channel already exists")
            else:
                insert=Channel_Details(channel_id)
                st.success(insert)

    if st.button("TO SQL"):
        tables=Tables()
        st.success(tables)

show_table=st.radio("SELECT THE TABLE TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_tables()
elif show_table=="PLAYLISTS":
    show_playlist_tables()
elif show_table=="VIDEOS":
    show_video_tables()
elif show_table=="COMMENTS":
    show_comment_tables()

#sql connection

mydb = psycopg2.connect(
    user='postgres',
    password='kabilajaisty',
    host='localhost',
    database='youtube',
    port=5432)
mycursor = mydb.cursor()

question=st.selectbox("SELECT QUESTION",("1. ALL VIDEOS AND THE CHANNLES",
                                         "2. CHANNELS WITH MOST NUMBER OF VIDEOS",
                                         "3. TOP 10 VIEWED VIDEOS",
                                         "4. COMMENTS IN EACH VIDEOS",
                                         "5. VIDEOS WITH HIGHEST LIKES",
                                         "6. LIKES OF ALL VIDEOS",
                                         "7. VIEWS OF EACH CHANNELS",
                                         "8. VIDEOS UPLOADED IN THE YEAR-2022",
                                         "9. AVERAGE DURATION OF ALL VIDEOS IN EACH CHANNEL",
                                         "10. VIDEOS WITH HIGHEST COMMENTS"))

mydb = psycopg2.connect(
    user='postgres',
    password='kabilajaisty',
    host='localhost',
    database='youtube',
    port=5432)
mycursor = mydb.cursor()

if question=="1. ALL VIDEOS AND THE CHANNLES":

    query1='''select videoid as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. CHANNELS WITH MOST NUMBER OF VIDEOS":
    query2='''select channel_name as channelname,VideoCount as no_videos from channels order by VideoCount desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. TOP 10 VIEWED VIDEOS":
    query3='''select View_Count as views, channel_name as channelname, VideoId as videotitle from videos
        where View_Count is not null order by views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)


elif question=="4. COMMENTS IN EACH VIDEOS":
    query4='''select Comment_Count as no_comment,Channel_Name as videotitle from videos where Comment_Count is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["comments","videotitle"])
    st.write(df4)

elif question=="5. VIDEOS WITH HIGHEST LIKES":
    query5='''select videoid as videotitle,Channel_Name as channelname,Like_Count as likecount from videos where Like_Count is not null
                order by Like_Count desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)


elif question=="6. LIKES OF ALL VIDEOS":
    query6='''select Like_Count as likecount,Channel_Name as channelname from videos'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["videotitle","channelname"])
    st.write(df6)


elif question=="7. VIEWS OF EACH CHANNELS":
    query7='''select viewCount as totalviews,Channel_Name as channelname from channels'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["totalviews","channelname"])
    st.write(df7)


elif question=="8. VIDEOS UPLOADED IN THE YEAR-2022":
    query8='''select videoid as title,Channel_Name as channelname,PublishedAt as uploadedon from videos 
    WHERE EXTRACT(YEAR FROM CAST(PublishedAt AS DATE)) = 2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["totalviews","channelname","uploadedon"])
    print(df8)
    st.write(df8)


elif question=="9. AVERAGE DURATION OF ALL VIDEOS IN EACH CHANNEL":
    query9='''select Channel_Name as channelname,AVG(Duration) as averageduration from videos group by Channel_Name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avg_duration=average_duration_str))
        df1=pd.DataFrame(T9)
        st.write(df1)


elif question=="10. VIDEOS WITH HIGHEST COMMENTS":
    query10='''select videoid as videoid, Channel_Name as channelname,Comment_Count as comments from videos where Comment_Count is
            not null order by Comment_Count desc'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videoid","channelname","comments"])
    st.write(df10)