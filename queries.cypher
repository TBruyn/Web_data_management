// Retrieve all songs with a duration strictly larger than 204 
// and smaller or equal to 205 seconds 
// having an end of fade in smaller than 0.5s

MATCH (s:Song {duration: d, endoffadein: eofi})
WHERE s.d > 204 AND d <= 205 AND eofi < 0.5
RETURN s

// Retrieve all different durations of fade-outs for 
// all songs having a tempo strictly larger than 100 
// and smaller then 130

MATCH (s:Song)
WHERE s.tempo > 100 AND s.duration < 130
RETURN s.duration

// Retrieve all songs ordered by the number of segments for artists 
// which released at least 10 songs before 1990

MATCH (a:Artist)--(s:Song)
WHERE s.year < 1990
WITH a, s, COLLECT(S) AS songnr
WHERE size(songnr)> 10
MATCH (a)--(s2:Song)
RETURN s2.name, s2.segmentscount ORDER BY s2.segmentscount DESC