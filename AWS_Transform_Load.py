import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


def album(data):
    album_list = []
    for item in data['items']:
        album_id = item['track']['album']['id']
        album_name = item['track']['album']['name']
        album_ReleaseDate = item['track']['album']['release_date']
        addedto_playlist = item['added_at']
        playlist_user_id = item['added_by']['id']
        Song_id = item['track']['id']
        
        album_list.append({'playlist_user_id':playlist_user_id,'album_id':album_id,'album_name':album_name,'album_ReleaseDate':album_ReleaseDate,'addedto_playlist':addedto_playlist,'Song_id':Song_id})


    return album_list

def song_artists(data):
    Song_artists = []
    for item in data['items']:
        song_id = item['track']['id']
        for artist in item['track']['artists']:
            artist_id = artist['id']
            artist_name = artist['name']
            
            Song_artists.append({'song_id':song_id,'artist_id':artist_id,'artist_name':artist_name})
        
    
    return Song_artists


def songdetails(data):
    Songdetails = []
    for item in data['items']:
        Song_id = item['track']['id']
        Song_name = item['track']['name']
        Song_popularity = item['track']['popularity']
        Song_duration_ms = item['track']['duration_ms']
        Songdetails.append({'Song_id':Song_id,'Song_name':Song_name,'Song_popularity':Song_popularity,'Song_duration_ms':Song_duration_ms})


    return Songdetails


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    Bucket='spotify-etl-abi28'
    key = 'raw-data/tobe-processed/'

    spotify_data = []
    spotify_file_name = []

    # print(s3.list_objects(Bucket= Bucket, Prefix = key)['Contents'][1:])

    for item in s3.list_objects(Bucket= Bucket, Prefix = key)['Contents']:
        filekey = item['Key']
        
        if filekey.split('.')[-1] == 'json':
            
            response = s3.get_object(Bucket=Bucket, Key=filekey)
            content = response['Body']
            data = json.loads(content.read())
            
            # print(data.keys([]))
            spotify_data.append(data)
            spotify_file_name.append(filekey)
            
    #         #transform data
    
    for data in spotify_data:
        album_data = album(data)
        album_df = pd.DataFrame.from_dict(album_data)

        albumkey = 'transformed-data/album-data/Album_details_'+str(datetime.now())+'.csv'
        albumbuffer = StringIO()
        album_df.to_csv(albumbuffer)
        albumcontent = albumbuffer.getvalue()
        s3.put_object(Bucket= Bucket, Key=albumkey, Body=albumcontent)

        song_artist_data = song_artists(data)
        song_artist_df = pd.DataFrame.from_dict(song_artist_data)

        artistkey = 'transformed-data/song-artists-data/Artist_details_'+str(datetime.now())+'.csv'
        artistbuffer = StringIO()
        song_artist_df.to_csv(artistbuffer)
        artistcontent = artistbuffer.getvalue()
        s3.put_object(Bucket= Bucket, Key=artistkey, Body=artistcontent)

        songdetail_data = songdetails(data)
        songdetail_df = pd.DataFrame.from_dict(songdetail_data)

        songkey = 'transformed-data/songdetails/Song_details_'+str(datetime.now())+'.csv'
        songbuffer = StringIO()
        songdetail_df.to_csv(songbuffer)
        songcontent = songbuffer.getvalue()
        s3.put_object(Bucket= Bucket, Key=songkey, Body=songcontent)


    s3_resource = boto3.resource('s3')
    for file in spotify_file_name:
        copy_source = {
            'Bucket': Bucket,
            'Key': file
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw-data/processed-data/'+ file.split('/')[-1])
        s3_resource.Object(Bucket, file).delete()
