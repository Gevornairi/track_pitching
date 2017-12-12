from google.cloud import bigquery
import requests
import pymongo
from pymongo import MongoClient
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/khondkg/Documents/keys/umg-dev-key.json'

client = bigquery.Client(project='umg-dev')
mongoClient = MongoClient('mongodb://gevornairi@localhost:27017/')
tempDb = mongoClient.temp
trackFeatureCollection = tempDb.track_info

distinctTracks = client.query('SELECT DISTINCT isrc FROM `umg-dev.pitching.spotify_playlists_4K_tracks_info`')
iterator = distinctTracks.result(timeout=6000)
rows = list(iterator)
creativeData = []
dictionary = {}

savedData = mongoClient.temp.track_info.find()

for track in savedData:
    dictionary[track['id']] = ''

for i in range(80278, len(rows) - 1):
    row = rows[i]
    isrc = row[0].encode('utf-8')
    if isrc not in dictionary:
        uri = 'https://pre-audition.umusic.net/api/v1/assets/{0}'.format(isrc)
        ircamData = requests.get(uri)
        if ircamData.ok is True:
            doc = trackFeatureCollection.insert_one({"id": isrc, "ircam": ircamData.text, "idx" : i})
            #creativeData.append((isrc, ircamData.text, None))
"""
dataset_ref = client.dataset('pitching')
pitching_dataset = client.get_dataset(dataset_ref)
track_creative_metadata_table_ref = pitching_dataset.table('track_creative_metadata')
track_creative_metadata_table = client.get_table(track_creative_metadata_table_ref)

errors = client.create_rows(track_creative_metadata_table, creativeData)

data = ''
"""
