import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    url = "https://open.spotify.com/playlist/40WMGBhEAUFf4lqEtP6oG9"
    playlist_uri = url.split('/')
    data = sp.playlist_tracks(playlist_uri[-1])


    client = boto3.client('s3')
    filename = 'spotify_'+playlist_uri[-1]+'_'+str(datetime.now())+'.json'

    client.put_object(
        Bucket='spotify-etl-abi28',
        Key='raw-data/tobe-processed/'+filename,
        Body=json.dumps(data)
    )
