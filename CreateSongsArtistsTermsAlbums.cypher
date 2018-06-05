USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///export.csv' AS songLine

// Create song
MERGE (song:Song {uniqueID: songLine.artistid})
ON CREATE SET
    song.songID=                     songLine.song,
    song.artist_id=                  songLine.artistid,
    song.analysissamplerate=         toInt(songLine.analysissamplerate),
    song.audiomd5=                   songLine.audiomd5,
    song.endoffadein=                toFloat(songLine.endoffadein),
    song.energy=                     toFloat(songLine.energy),
    song.release=                    songLine.release,
    song.release7digitalid=          toInt(songLine.release7digitalid),
    song.songhottness=               toFloat(songLine.songhotness),
    song.loudnes=                    toFloat(songLine.loudnes),
    song.mode=                       toInt(songLine.mode),
    song.modeconfidende=             toFloat(songLine.modeconfidence),
    song.danceability=               toFloat(songLine.danceability),
    song.duration=                   toFloat(songLine.duration),
    song.keysignature=               toInt(songLine.keysignature),
    song.keysignature_confidence=    toFloat(songLine.keysignatureconfidence),
    song.tempo=                      toFloat(songLine.tempo),
    song.timesignature=              toInt(songLine.timesignature),
    song.timesignature_confidence=   toFloat(songLine.timesignatureconfidence),
    song.title=                      songLine.title,
    song.year=                       toInt(songLine.year),
    song.trackid=                    songLine.trackid
    song.segmentscount=              songLine.segmentscount

// Create artist
MERGE (artist:Artist {uniqueID: 'id' + songLine.artistid})
ON CREATE SET
    artist.name=                   songLine.artistname,
    artist.artist_id=              songLine.artistid,
    artist.longitude=              toFloat(songLine.artistlongitude),
    artist.latitude=               toFloat(songLine.artistlatitude),
    artist.mbid=                   songLine.artistmbid,
    artist.playmeiid=              toInt(songLine.artistplaymeid),
    artist.artist7digitalid=       toInt(songLine.artist7digitalid),
    artist.artistfamiliarity=      toFloat(songLine.artistfamiliarity),
    artist.artisthotness=        toFloat(songLine.artisthottness),
    artist.mbtags=                 CASE WHEN size(songLine.mbtags) > 4 
                                   THEN
                                    filter( x IN split(
                                   substring(songLine.mbtags, 1, 
                                   size(songLine.mbtags) - 2),
                                   '\'') WHERE size(trim(x))>0 )
                                   END,
    artist.terms=                  CASE WHEN size(songLine.artistterms) > 4 
                                   THEN
                                   filter( x IN split(
                                   substring(songLine.artistterms, 2, 
                                   size(songLine.artistterms) - 3),
                                   '\' \'') WHERE size(trim(x))>0 )
                                   ELSE
                                   [] 
                                   END,
    artist.terms_freq=             CASE WHEN size(songLine.artistterms) > 4 
                                   THEN
                                    extract(n IN filter( x IN split(
                            substring(songLine.artisttermsfreq, 1, size(songLine.artisttermsfreq) - 2),
                            ' ') WHERE size(trim(x))>0 )
                            | toFloat(n))
                                    END,
    artist.terms_weight=           CASE WHEN size(songLine.artistterms) > 4 
                                   THEN
                                   extract(n IN filter( x IN split(
                            substring(songLine.artisttersmweights, 1, size(songLine.artisttermsweights) - 2),
                            ' ') WHERE size(trim(x))>0 )
                            | toFloat(n))
                                    END,
    artist.similarartistsstrings=  CASE WHEN size(songLine.artistterms) > 4 
                                   THEN
                                    filter( x IN split(
                                    substring(songLine.similarartists, 1, size(songLine.similarartists) - 2),
                                    '\'') WHERE size(trim(x))>0 )
                                    ELSE
                                    []
                                    END

MERGE (artist)-[:WROTE_SONG]->(song)

// Create terms that are linked to the artist
FOREACH (i IN RANGE(0, size(artist.terms) - 1) |
    MERGE (term:Term {name: artist.terms[i]})
    MERGE (artist)-[assoc:ASSOCIATED_WITH]->(term)
    ON CREATE SET 
        assoc.freq      = artist.terms_freq[i],
        assoc.weight    = artist.terms_weight[i]
)

// Create albums with a relationship to an artist
MERGE (album:Album {
    id:     songLine.albumid
})
ON CREATE SET album.name = songLine.albumname

MERGE (artist)-[:WROTE_ALBUM]->(album)

// Create relationship between song and album
MERGE (song)-[:IN_ALBUM]->(album)

// Create relationship between similar artists
WITH artist AS artist, song As song

UNWIND artist.similarartistsstrings AS similarID

MATCH (similarArtist:Artist {artist_id: similarID})

MERGE (artist)-[:SIMILAR]->(similarArtist);