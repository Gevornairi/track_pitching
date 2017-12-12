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

distinctTracks = client.query("SELECT uri, Max(isrc) as isrc FROM `umg-dev.pitching.pitching_tracks` WHERE TRIM(uri) > '' GROUP BY uri")
rows = list(distinctTracks.result(timeout=6000))
dictionary = {}

savedData = mongoClient.temp.track_info.find()

for track in savedData:
    track_id = track.get('id')
    if track_id:
        dictionary[track_id] = track
    else:
        dictionary[track['track_uri']] = track

encoded = base64.b64encode('8bf31530fc88420483488e05f9a9a5ec:513f422fa2db483f8de3b27049febaf7')

tokenRequest = requests.post('https://accounts.spotify.com/api/token',
    data={'grant_type' : 'client_credentials'}, headers={'Authorization' : 'Basic {0}'.format(encoded)})

tokenJson = json.loads(tokenRequest.text)

token = tokenJson['access_token']

mapping = {}

while True:

    if (len(mapping) >= 95) or (len(rows) <= 0):
        echonestRequest = requests.get('https://api.spotify.com/v1/audio-features/?ids={0}'.format(','.join(mapping.keys())), headers={'Authorization' : 'Bearer {0}'.format(token)})
        if echonestRequest.ok:
            data = json.loads(echonestRequest.text)['audio_features']
            for echonest_data in data:
                track_info = None
                if echonest_data is None:
                    continue

                spotify_id = echonest_data.get('id')

                mapped_isrc = mapping.get(spotify_id)
                if mapped_isrc:
                    track_info = dictionary.get(mapped_isrc)
                else:
                    track_info = dictionary.get(echonest_data['uri'])

                if track_info is not None:
                    track_info['track_uri'] = echonest_data['uri']
                    track_info['echonest'] = echonest_data
                    doc = trackFeatureCollection.update_one({'_id': track_info['_id']}, { '$set': track_info })
                else:
                    doc = trackFeatureCollection.insert_one({"id": mapped_isrc, "track_uri": echonest_data['uri'], "echonest": echonest_data})
            mapping = {}

    if len(rows) <= 0:
        break

    dataRow = rows.pop()
    track_uri = dataRow[0].encode('utf-8')
    track_id = track_uri.split(':')[2]
    isrc = dataRow[1].encode('utf-8')

    trackRow = None

    if isrc is not None:
        trackRow = dictionary.get(isrc)
    else:
        trackRow = dictionary.get(track_uri)

    if (trackRow is None) or (trackRow.get('echonest') is None):
        mapping[track_id] = isrc



#doc = trackFeatureCollection.insert_one({"id": isrc, "ircam": ircamData.text, "idx" : i})
