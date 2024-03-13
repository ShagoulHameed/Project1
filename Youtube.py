from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
from pymongo import MongoClient
import streamlit as st

# Api key connect

def api_Connect():
    API_KEY = "AIzaSyDgxMekwxh0_cY9laHQsmzti3cNgczJQzQ"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=API_KEY)
    return youtube

youtube = api_Connect()

# channel info
def get_channelinfo(Channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=Channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_name=i["snippet"]["title"],
                Channel_Id=i["id"],
                subscribers=i["statistics"]["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_Video = i["statistics"]["videoCount"],
                Channel_Descr=i["snippet"]["description"],
                Playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"]
                )
    return  data    


def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part="contentDetails"
    ).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response_items = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in response_items['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        
        next_page_token = response_items.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                Channnel_name=item['snippet']['channelTitle'],
                Channel_id=item['snippet']['channelId'],
                Video_ID=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags', 0),
                Thumbnails=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet']['description'],
                Publishdata=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Viwes=item['statistics'].get('viewCount', 0),
                Comments=item['statistics'].get('commentCount', 0),
                Likes = item['statistics'].get('likeCount', 0),
                Favorite_count=item['statistics'].get('favoriteCount', 0),
                definition=item['contentDetails']['definition'],
                CaptionStatus=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data
    

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:

            request = youtube.commentThreads().list(
                    part='snippet',
                    videoId= video_id,
                    maxResults=50,
                    
                )

            response = request.execute()

            for item in response['items']:
                data = dict(Comment_id=item['snippet']['topLevelComment']['id'],
                            VideoID = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Commetedtext= item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            commentowener=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Commentpostedon=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

    except:
            pass
    return Comment_data

def get_Playlist_info(Channel_id):
    nextpageToken = None
    Alldatalist = []
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=Channel_id,
            maxResults=50,
            pageToken=nextpageToken
        )
        response = request.execute()

        for item in response['items']:
            data = {
                'PlaylistID': item['id'],
                'Title': item['snippet']['title'],
                'ChannelID': item['snippet']['channelId'],
                'channelname': item['snippet']['channelTitle'],
                'publishedat': item['snippet']['publishedAt'],
                'Video_count': item['contentDetails']["itemCount"]
            }
            Alldatalist.append(data)
        
        nextpageToken = response.get('nextPageToken')
        if nextpageToken is None:
            break
    return Alldatalist


#dataupload mongodb
client=pymongo.MongoClient("mongodb+srv://shagoul04:Wt8NI6Qwc1nEsCaI@cluster0.8syjnh2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db=client["Youtube_data"]


def channel_details(Channel_id):
    Ch_detaisl=get_channelinfo(Channel_id)
    Pl_details= get_Playlist_info(Channel_id)
    Vii_ids = get_video_ids(Channel_id)
    vi_details= get_video_info(Vii_ids)
    com_detisl= get_comment_info(Vii_ids)

    collection1=db["channel_details"]
    collection1.insert_one({"Channel_infromation":Ch_detaisl,"Playlist_information":Pl_details,
                            "Video_infromation":vi_details,"Comment_inforamtion":com_detisl})
    
    return  "I've uploaded the data’s into mongoDb!!"


# Collection fro chan|play|Vid|comm
def channels_Table(Channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="0040",
                            database="YoutbuData",
                            port= "5432")
    cursor = mydb.cursor()

    #drop_query = '''drop table if exists channels'''
    #cursor.execute(drop_query)
    #mydb.commit()

   
    create_query = '''create table if not exists channels(Channel_name varchar(100),
                        Channel_Id varchar(100) primary key, 
                        subscribers bigint,
                        Views bigint,
                        Total_Video int ,
                        Channel_Descr text,
                        Playlist_ID varchar(100))'''
    cursor.execute(create_query)
    mydb.commit()



    single_channel_detail = []
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({"Channel_infromation.Channel_name": Channel_name_s},{'_id':0}):
        single_channel_detail.append(ch_data["Channel_infromation"])

    df_single_channel_detail = pd.DataFrame(single_channel_detail)


    for index,row in df_single_channel_detail.iterrows():
        insert_qury = '''insert into channels(Channel_name,
                            Channel_Id,
                            subscribers,
                            Views,
                            Total_Video,
                            Channel_Descr,
                            Playlist_ID)
                            
                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_Id'],
                row['subscribers'],
                row['Views'],
                row['Total_Video'],
                row['Channel_Descr'],
                row['Playlist_ID'])
        
        try:

            cursor.execute(insert_qury,values)
            mydb.commit()

        except:

            news = f"Your given channel is {Channel_name_s} is Alrady Exists!! "
            return news







def playlist_table_new(Channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="0040",
                            database="YoutbuData",
                            port="5432")
    cursor = mydb.cursor()


    create_query = '''create table if not exists Playlist(PlaylistID varchar(100) primary key,
                            Title varchar(100), 
                            ChannelID varchar(100),
                            channelname varchar(100),
                            publishedat timestamp,
                            Video_count int)'''

    cursor.execute(create_query)
    mydb.commit()

    single_playlist_detisl = []
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({"Channel_infromation.Channel_name": Channel_name_s},{'_id':0}):
        single_playlist_detisl.append(ch_data["Playlist_information"])

    df_single_playlist_detisl = pd.DataFrame(single_playlist_detisl[0])

    for index,row in df_single_playlist_detisl.iterrows():
        insert_qury = '''insert into Playlist(PlaylistID,
                            Title, 
                            ChannelID,
                            channelname,
                            publishedat,
                            Video_count)
                            
                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['PlaylistID'],
                row['Title'],
                row['ChannelID'],
                row['channelname'],
                row['publishedat'],
                row['Video_count'])
        
        
        cursor.execute(insert_qury,values)
        mydb.commit()      


#Comment table
def videos_table(Channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="0040",
                            database="YoutbuData",
                            port= "5432")
    cursor = mydb.cursor()


        
    create_query = '''create table if not exists Videos(Channnel_name varchar(100),
                                                        Channel_id  varchar(100),
                                                        Video_ID  varchar(100) primary key,
                                                        Title  varchar(200),
                                                        Tags text,
                                                        Thumbnails  varchar(300),
                                                        Description text,
                                                        Publishdata timestamp,
                                                        Duration interval,
                                                        Viwes bigint,
                                                        Comments int,
                                                        Likes bigint,
                                                        Favorite_count int,
                                                        definition  varchar(100),
                                                        CaptionStatus  varchar(100))'''


    cursor.execute(create_query)
    mydb.commit()

    single_video_detisl = []
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({"Channel_infromation.Channel_name": Channel_name_s},{'_id':0}):
        single_video_detisl.append(ch_data["Video_infromation"])
    df_single_video_detisl = pd.DataFrame(single_video_detisl[0])


    for index,row in df_single_video_detisl.iterrows():
            insert_qury = '''insert into Videos(Channnel_name,
                                            Channel_id,
                                            Video_ID,
                                            Title,
                                            Tags,
                                            Thumbnails,
                                            Description,
                                            Publishdata,
                                            Duration,
                                            Viwes,
                                            Comments,
                                            Likes,
                                            Favorite_count,
                                            definition,
                                            CaptionStatus)
                                            
                    
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    
            values=(row['Channnel_name'],
                    row['Channel_id'],
                    row['Video_ID'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnails'],
                    row['Description'],
                    row['Publishdata'],
                    row['Duration'],
                    row['Viwes'],
                    row['Comments'],
                    row['Likes'],
                    row['Favorite_count'],
                    row['definition'],
                    row['CaptionStatus']
                    )
            cursor.execute(insert_qury,values)
            mydb.commit()


def commentsNEW_table(Channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="0040",
                            database="YoutbuData",
                            port="5432")
    cursor = mydb.cursor()


    create_query = '''create table if not exists Comments(Comment_id varchar(100) primary key,
                                    VideoID varchar(100),
                                    Commetedtext text,
                                    commentowener varchar(100),
                                    Commentpostedon timestamp
                            )'''

    cursor.execute(create_query)
    mydb.commit()

    single_commnets_detisl = []
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({"Channel_infromation.Channel_name": Channel_name_s},{'_id':0}):
        single_commnets_detisl.append(ch_data["Comment_inforamtion"])
    df_single_commnets_detisl = pd.DataFrame(single_commnets_detisl[0])

    for index, row in df_single_commnets_detisl.iterrows():
        insert_query = '''insert into Comments(Comment_id,
                                        VideoID,
                                        Commetedtext,
                                        commentowener,
                                        Commentpostedon)
                                values(%s,%s,%s,%s,%s)'''
        values = (row['Comment_id'],
                  row['VideoID'],
                  row['Commetedtext'],
                  row['commentowener'],
                  row['Commentpostedon'])

        cursor.execute(insert_query, values)
        mydb.commit()


def alltable(single_channel):

    news  = channels_Table(single_channel)

    if news:
        return news
    
    else:
        playlist_table_new(single_channel)
        videos_table(single_channel)
        commentsNEW_table(single_channel)

        return "The table has been successfully created."


def show_channel_table():
    ch_list = []
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{'_id':0,"Channel_infromation":1}):
        ch_list.append(ch_data["Channel_infromation"])
    df=st.dataframe(ch_list)

    return df


def show_playlist_table():
    Pl_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for Pl_data in collection1.find({}, {'_id': 0, "Playlist_information": 1}):
            for i in range(len(Pl_data["Playlist_information"])):
                Pl_list.append(Pl_data["Playlist_information"][i])            
    df1 = st.dataframe(Pl_list)

    return df1
    

def show_video_table():
    VI_list = []
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for VI_data in collection1.find({},{'_id':0,"Video_infromation":1}):
        for i in range(len(VI_data["Video_infromation"])):
            VI_list.append(VI_data["Video_infromation"][i])
    df2 = st.dataframe(VI_list)

    return df2


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for com_data in collection1.find({}, {'_id': 0, "Comment_inforamtion": 1}):
        for i in range(len(com_data["Comment_inforamtion"])):                        
            com_list.append(com_data["Comment_inforamtion"][i])            
    df5 = st.dataframe(com_list)

    return df5



# streamlit 
st.markdown(
    """
    <style>
    .sidebar .sidebar-content {
        background-color: #4c89c7; 
        padding: 20px;
        color: white; 
        font-family: Arial, sans-serif;
    }
    .sidebar .sidebar-content a {
        color: #ffffff; 
        text-decoration: none; 
    }
    .sidebar .sidebar-content .sidebar-header {
        text-align: center;
        margin-bottom: 20px;
    }
    .sidebar .sidebar-content .sidebar-header h1 {
        font-size: 24px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .sidebar .sidebar-content .sidebar-header p {
        font-size: 14px;
    }
    .sidebar .sidebar-content .sidebar-skills {
        margin-bottom: 20px;
    }
    .sidebar .sidebar-content .sidebar-skills ul {
        list-style-type: none;
        padding-left: 0;
        margin: 0;
    }
    .sidebar .sidebar-content .sidebar-skills ul li {
        padding: 5px 0;
    }
    .sidebar .sidebar-content .sidebar-button {
        text-align: center;
    }
    .sidebar .sidebar-content .sidebar-button button {
        background-color: #3498db; 
        color: white; 
        font-weight: bold;
        border: none;
        border-radius: 5px;
        padding: 10px 20px;
        cursor: pointer;
    }
    .sidebar .sidebar-content .sidebar-button button:hover {
        background-color: #2980b9; /* Button background color on hover */
    }
    </style>
    """,
    unsafe_allow_html=True
)

with st.sidebar:
    st.markdown("<div class='sidebar-header'>", unsafe_allow_html=True)
    st.markdown("<h1 style='color: #FF0000;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)
    st.markdown("<p>Technology used:</p>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.image("Python.png", width=150)  
    st.image("mangodb.png", width=150)  
    st.image("API.jpg", width=150)  
    st.image("SQL.jpg", width=150) 
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='sidebar-button'>", unsafe_allow_html=True)
    if st.button("Learn More"):
        st.markdown("<a href='https://example.com'>Click here to learn more</a>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# Main content
st.image("youtubu.jpg", width=350)



channel_id = st.text_input("Enter the Channel ID:")

if st.button("Check and Save Channel Data"):
    ch_ids = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({},{'_id':0,"Channel_infromation":1}):
        ch_ids.append(ch_data["Channel_infromation"]["Channel_Id"]) 

    if channel_id in ch_ids:
        st.success("This channel has already been stored in the database.")
    else:
        insert = channel_details(channel_id)    
        st.success(insert)

all_channel = []
db=client["Youtube_data"]
collection1=db["channel_details"]
for ch_data in collection1.find({},{'_id':0,"Channel_infromation":1}):
    all_channel.append(ch_data["Channel_infromation"]["Channel_name"])        

uniquechannel  = st.selectbox("Please Select The Channel",all_channel)

if st.button("Migrate Channel Data to SQL"):
    table = alltable(uniquechannel)
    st.success(table)

show_table = st.radio("Choose a table to view", ("Channel", "Playlist", "Videos", "Comments"))

if show_table == "Channel":
    show_channel_table()

if show_table == "Playlist":
    show_playlist_table()

if show_table == "Videos":
    show_video_table()

if show_table == "Comments":
    show_comments_table()


#SQL connect

mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="0040",
                        database="YoutbuData",
                        port="5432")
cursor = mydb.cursor()

questions = st.selectbox("select your  question",["1. What are the names of all the videos and their corresponding channels?",
                         
                         "2. Which channels have the most number of videos, and how many videos do they have?",
                         "3. What are the top 10 most viewed videos and their respective channels?",
                         "4. How many comments were made on each video, and what are their corresponding video names?",
                         "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                         "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                         "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                         "8. What are the names of all the channels that have published videos in the year 2022?",
                         "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                         "10. Which videos have the highest number of comments, and what are their corresponding channel names?"])

if questions=="1. What are the names of all the videos and their corresponding channels?":
    query1 = '''select title as videos,Channnel_name as channelname from Videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos title","channel name"])
    st.write(df)

elif questions=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''select Channel_name as channelname,Total_Video as Number_videos from channels
    order by Total_Video desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","Number of videos"])
    st.write(df2)    
elif questions=="3. What are the top 10 most viewed videos and their respective channels?":
    query3 = '''select Viwes as Viwes,Channnel_name as channelname,Title as video_title from Videos
    where Viwes is not null order by Viwes desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Viwes","channel name","video title"])
    st.write(df3)    
elif questions=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select Comments as number_comments,Title as video_title from Videos
    where Comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of Comments","video_title"])
    st.write(df4)    
elif questions=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select Title as video_title,Channnel_name as Channnelname,Likes as Likecount from Videos
    where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Video Title","Channnel name","Likes"])    
    st.write(df5)
elif questions=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''select Likes as Likescount,Title as video_Title from Videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Likes Count","Video Title"])
    st.write(df6)

elif questions=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select Channel_name as Channelname,Views as totalViews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel name","Total Views"])   
    st.write(df7)
elif questions=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select Title as vide_Title,Publishdata as releasedate,Channnel_name as Channnelname from Videos
                    where extract(year from Publishdata)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video Title","Published date","Channnel name"])    
    st.write(df8)
elif questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''SELECT Channnel_name AS Channnelname, AVG(Duration) AS averageduration FROM Videos GROUP BY Channnel_name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["Channnelname", "averageduration"])

    #st.write("Column Names:", df9.columns.tolist())  
    
    T9 = []
    for index, row in df9.iterrows():
        Channnel_title = row["Channnelname"]  
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append({"Channneltitle": Channnel_title, "average_duration": average_duration_str})
    df99 = pd.DataFrame(T9)  
    st.write(df99)
elif questions=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select Title as video_Title, Channnel_name as Channnelname, Comments as Comments from Videos
                where Comments is not null order by Comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video Title","Channnel name","Comments"])
    st.write(df10)    