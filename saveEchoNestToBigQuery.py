from google.cloud import bigquery
import requests
import base64
import json
import pymongo
from pymongo import MongoClient
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/khondkg/Documents/keys/umg-dev-key.json'

client = bigquery.Client(project='umg-dev')
mongoClient = MongoClient('mongodb://gevornairi@localhost:27017/')
tempDb = mongoClient.temp
trackFeatureCollection = tempDb.track_info

tracks_info = mongoClient.temp.track_info.find()

dataset_ref = client.dataset('pitching')
pitching_dataset = client.get_dataset(dataset_ref)
track_creative_metadata_table_ref = pitching_dataset.table('pitch_track_echonest_data')
track_creative_metadata_table = client.get_table(track_creative_metadata_table_ref)

ROWS_TO_INSERT = []

save_idx = 0

for track in tracks_info:
    echonest_data = track.get('echonest')
    if echonest_data is not  None:
        ROWS_TO_INSERT.append((track.get('id'), echonest_data.get('uri'), echonest_data.get('energy'), echonest_data.get('liveness'),
                               echonest_data.get('tempo'), echonest_data.get('speechiness'), echonest_data.get('acousticness'), echonest_data.get('instrumentalness'),
                               echonest_data.get('danceability'), echonest_data.get('key'), echonest_data.get('duration_ms'),
                               echonest_data.get('loudness'), echonest_data.get('valence'), echonest_data.get('mode'), None))
        save_idx = save_idx + 1
        if save_idx >= 10000:
            errors = client.create_rows(track_creative_metadata_table, ROWS_TO_INSERT)
            save_idx = 0
            ROWS_TO_INSERT = []


if len(ROWS_TO_INSERT) > 0:
    errors = client.create_rows(track_creative_metadata_table, ROWS_TO_INSERT)
