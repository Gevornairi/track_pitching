from google.cloud import bigquery
from collections import defaultdict
import csv
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/khondkg/Documents/keys/umg-dev-key.json'

uri_name = 'uri'
energy_name = 'energy'
liveness_name = 'liveness'
tempo_name = 'tempo'
speechiness_name = 'speechiness'
acousticness_name = 'acousticness'
instrumentalness_name = 'instrumentalness'
danceability_name = 'danceability'
key_name = 'key'
duration_ms_name = 'duration_ms'
loudness_name = 'loudness'
valence_name = 'valence'
mode_name = 'mode'
release_year_name = 'release_year'
track_artist_name = 'track_artist'
genre_name = 'genre'
track_country_name = 'track_country'

def get_track_value(track_data, key):
    current_value = 0.0
    discret_feature = False

    if key.startswith('mode_'):
        if track_data.get(mode_name) and 'mode_{0}'.format(track_data.get(mode_name)) == key:
            current_value = 1
        discret_feature = True
    elif key.startswith('year_'):
        if track_data.get(release_year_name) and 'year_{0}'.format(track_data.get(release_year_name)) == key:
            current_value = 1
        discret_feature = True
    elif key.startswith('artist_'):
        if track_data.get(track_artist_name) and 'artist_{0}'.format(track_data.get(track_artist_name)) == key:
            current_value = 1
        discret_feature = True
    elif key.startswith('genre_'):
        if track_data.get(genre_name) and 'genre_{0}'.format(track_data.get(genre_name)) == key:
            current_value = 1
        discret_feature = True
    elif key.startswith('key_'):
        if track_data.get(key_name) and 'key_{0}'.format(track_data.get(key_name)) == key:
            current_value = 1
        discret_feature = True
    elif key.startswith('country_feature_'):
        if track_data.get(track_country_name) and 'country_feature_{0}'.format(track_data.get(track_country_name)) == key:
            current_value = 1
        discret_feature = True
    else:
        if track_data.get(key):
            current_value = track_data.get(key)

    return current_value, discret_feature


playlistsDictionary = {}
playlistDistinctTracksDictionary = {}
result_array = []
tracks_array = []

client = bigquery.Client(project='umg-dev')

# Load Playlists From BigQuery
playlists = list(client.query("SELECT playlist_uri FROM `umg-dev.pitching.spotify_playlists_info_4K` WHERE LENGTH(playlist_uri) > 0 LIMIT 10").result(timeout=6000))

# Fill Playlists to Dictionary
for playlist in playlists:
    uri = playlist[0].encode('utf-8')
    id = uri.split(':')[-1]
    playlistsDictionary[id] = (uri, [], defaultdict(float), defaultdict(float), 0.0)

# Load All Playlist Distinct Tracks
playlistDistinctTracks = list(client.query("SELECT * FROM `umg-dev.pitching.playlist_tracks_echonest_data`").result(timeout=6000))

# Fill Playlist Distinct Tracks to Dictionary
for track in playlistDistinctTracks:
    isrc = track[0].encode('utf-8')
    uri = track[1].encode('utf-8')
    energy = track[2]
    liveness = track[3]
    tempo_raw = track[4]
    tempo = None
    if tempo_raw:
        tempo = (tempo_raw - 0.0) / ((248.113 - 0.0) * 1.0)
    speechiness = track[5]
    acousticness = track[6]
    instrumentalness = track[7]
    danceability = track[8]
    key = track[9].encode('utf-8') if track[9] else None
    duration_ms = track[10].encode('utf-8') if track[10] else None
    loudnessRaw = track[11]
    loudness = (loudnessRaw + 60) / (60 * 1.0)
    valence = track[12]
    mode = track[13].encode('utf-8') if track[13] else None
    release_date_raw = track[14].encode('utf-8') if track[14] else None
    release_year = None
    if release_date_raw:
        release_year = release_date_raw.split(' ')[0]

    track_artist = track[15].encode('utf-8') if track[15] else None
    genre = track[17].encode('utf-8') if track[17] else None

    track_country = isrc[0:2]

    playlistDistinctTracksDictionary[isrc] ={uri_name : uri, energy_name: energy, liveness_name: liveness,
                                             tempo_name: tempo, speechiness_name: speechiness, acousticness_name: acousticness,
                                             instrumentalness_name: instrumentalness, danceability_name: danceability,
                                             key_name:key, duration_ms_name: duration_ms, loudness_name: loudness,
                                             valence_name:valence, mode_name: mode, release_year_name: release_year,
                                             track_artist_name: track_artist, genre_name: genre, track_country_name: track_country}

playlist_tracks_query =  "SELECT playlist_id, isrc FROM `umg-dev.pitching.spotify_playlists_4K_tracks_info` WHERE LENGTH(isrc) > 0 AND playlist_id IN ({0})".format(",".join("'{0}'".format(w) for w in playlistsDictionary.keys()))

# Load All Playlist Tracks
playlistTracks = list(client.query(playlist_tracks_query).result(timeout=6000))

# Append For Each Playlist Their Tracks
for playlistTrack in playlistTracks:
    playlistId = playlistTrack[0].encode('utf-8')

    if not playlistsDictionary.get(playlistId):
        continue

    isrc = playlistTrack[1].encode('utf-8')
    playlistData = playlistsDictionary[playlistId]
    playlistData[1].append(isrc)
    playlistsDictionary[playlistId] = playlistData

for key, value in playlistsDictionary.iteritems():
    playlistTracks = value[1]
    idx = 0
    for isrc in playlistTracks:
        idx = idx + 1
        if idx <= 3:
            tracks_array.append((isrc, key))
            playlistTracks.remove(isrc)
        else:
            break

for playlistKey in playlistsDictionary:
    playlistTracks = playlistsDictionary[playlistKey][1]
    playlistMedian = playlistsDictionary[playlistKey][2]
    playlistWeights =playlistsDictionary[playlistKey][3]

    # Calculate Playlist Median Values
    for isrc in playlistTracks:
        trackData = playlistDistinctTracksDictionary.get(isrc)

        if not trackData:
            continue

        if trackData.get(energy_name):
            playlistMedian[energy_name] = playlistMedian[energy_name] + trackData[energy_name]

        if trackData.get(liveness_name):
            playlistMedian[liveness_name] = playlistMedian[liveness_name] + trackData[liveness_name]

        if trackData.get(tempo_name):
            playlistMedian[tempo_name] = playlistMedian[tempo_name] + trackData[tempo_name]

        if trackData.get(speechiness_name):
            playlistMedian[speechiness_name] = playlistMedian[speechiness_name] + trackData[speechiness_name]

        if trackData.get(acousticness_name):
            playlistMedian[acousticness_name] = playlistMedian[acousticness_name] + trackData[acousticness_name]

        if trackData.get(instrumentalness_name):
            playlistMedian[instrumentalness_name] = playlistMedian[instrumentalness_name] + trackData[instrumentalness_name]

        if trackData.get(danceability_name):
            playlistMedian[danceability_name] = playlistMedian[danceability_name] + trackData[danceability_name]

        if trackData.get(key_name):
            key_name_data = 'key_{0}'.format(trackData[key_name])
            playlistMedian[key_name_data] = playlistMedian[key_name_data] + 1

        if trackData.get(loudness_name):
            playlistMedian[loudness_name] = playlistMedian[loudness_name] + trackData[loudness_name]

        if trackData.get(valence_name):
            playlistMedian[valence_name] = playlistMedian[valence_name] + trackData[valence_name]

        if trackData.get(release_year_name):
            release_year_name_data = 'year_{0}'.format(trackData[release_year_name])
            playlistMedian[release_year_name_data] = playlistMedian[release_year_name_data] + 1

        if trackData.get(track_artist_name):
            artist_name_data = 'artist_{0}'.format(trackData[track_artist_name])
            playlistMedian[artist_name_data] = playlistMedian[artist_name_data] + 1

        if trackData.get(genre_name):
            genre_name_data = 'genre_{0}'.format(trackData[genre_name])
            playlistMedian[genre_name_data] = playlistMedian[genre_name_data] + 1

        if trackData.get(track_country_name):
            track_country_data = 'country_feature_{0}'.format(trackData.get(track_country_name))
            playlistMedian[track_country_data] = playlistMedian[track_country_data] + 1


    for key, value in playlistMedian.iteritems():
        playlistMedian[key] = value / (len(playlistTracks) * 1.0)

    # Calculate Playlist Sum Weights
    for isrc in playlistTracks:
        trackData = playlistDistinctTracksDictionary.get(isrc)

        if not trackData:
            continue

        for key, value in playlistMedian.iteritems():
            track_value, discrete = get_track_value(trackData, key)
            weight_value = 0.0
            if discrete == True and track_value == 0:
                weight_value = max(value, (1-value)) **2
            else:
                weight_value = (track_value - value)**2
            playlistWeights[key] = playlistWeights[key] + weight_value

    # Calculate Playlist Weights
    for key, value in playlistWeights.iteritems():
        weight_final_value = 1/(value * 1.0) if value != 0 else 1000
        playlistWeights[key] = weight_final_value

    # Calculate Max Error Of the Playlist
    playlist_max_error = 0.0

    for isrc in playlistTracks:
        trackData = playlistDistinctTracksDictionary.get(isrc)
        calc_sum = 0.0
        weight_sum = 0.0

        if not trackData:
            continue

        for key, value in playlistMedian.iteritems():
            track_value, discrete = get_track_value(trackData, key)
            calc_sum = calc_sum + (((track_value - value) ** 2) * playlistWeights[key])
            weight_sum = weight_sum + playlistWeights[key]

        result_value = calc_sum / (weight_sum * 1.0)

        if result_value > playlist_max_error:
            playlist_max_error = result_value

    playlistsDictionary[playlistKey] =(playlistsDictionary[playlistKey][0],
                                       playlistTracks, playlistMedian, playlistWeights, playlist_max_error)

right_pitching_count = 0

for pitching_track in tracks_array:
    isrc = pitching_track[0]
    playlist_id = pitching_track[1]
    pitching_track_dictionary = playlistDistinctTracksDictionary[isrc]


    for playlist_to_pitch_key, playlist_to_pitch_value  in playlistsDictionary.iteritems():
        playlist_uri = playlist_to_pitch_value[0]
        playlist_median = playlist_to_pitch_value[2]
        playlist_weights = playlist_to_pitch_value[3]
        playlist_max_error = playlist_to_pitch_value[4]
        weight_sum = 0.0
        calc_sum = 0.0

        for key, value in playlist_median.iteritems():
            track_value, discrete = get_track_value(pitching_track_dictionary, key)
            calc_sum = calc_sum + (((track_value - value)**2) * playlist_weights[key])
            weight_sum = weight_sum + playlist_weights[key]

        track_error = calc_sum / (weight_sum * 1.0)

        if track_error <= playlist_max_error:
            if playlist_to_pitch_key == playlist_id:
                right_pitching_count = right_pitching_count + 1

error_percentage = (100 - (100 * (right_pitching_count / len(tracks_array) * 1.0)))

print 'Error is for {0}%'.format(error_percentage)









