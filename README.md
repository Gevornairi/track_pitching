# track_pitching
Track Recommendations to Spotify popular playlists.

The project has a several files which are helping in the data preparation phase and also in the analysis phase.

1. The Main analysis is done in the analyze.py file which just executing it will load data from UMG Big Query to start       processing. To load the data neccessary access file is needed to have a access to the UMG Big Query.

2. Tested also approach to do analysis with IRCAM data but the problem was with this approach not enough data was there. The loading process for IRCAM has been done in the InitializeIRCAMData.py file.

3. In the echoNest.py file was loaded all Echo nest track metadata information for playlist tracks and for pitching tracks.

4. In the saveEchoNestToBigQuery.py file has been done moving all echo nest data previewsly loaded to the local mongodb database to Google Big Query

5. errorAnalysis.py file is the analysis of the error of the project, so some tracks are excluded from each playlists and then algorithm should recommend them back.




