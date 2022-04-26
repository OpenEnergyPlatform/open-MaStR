/*
open_FRED MaStR Solar Cleansing

Create a copy
Reset geom if outside of Germany
Allocate to centre of PLZ-area

__copyright__   = "© Reiner Lemoine Institut"
__license__ = "Creative Commons Zero v1.0 Universal (CC0-1.0)"
__url__     = "https://creativecommons.org/publicdomain/zero/1.0/"
__author__  = "Ludwig Hülk"
*/

-- Create a copy
DROP TABLE IF EXISTS sandbox.bnetza_mastr_solar_v1_3_clean CASCADE;
CREATE TABLE         sandbox.bnetza_mastr_solar_v1_3_clean AS
    SELECT  * 
    FROM    sandbox.bnetza_mastr_solar_v1_3
    ORDER BY id;

ALTER TABLE sandbox.bnetza_mastr_solar_v1_3_clean
    ADD PRIMARY KEY (id),
    ADD COLUMN geom geometry(Point,4326),
    ADD COLUMN "comment" text;

CREATE INDEX bnetza_mastr_solar_v1_3_clean_geom_idx
    ON sandbox.bnetza_mastr_solar_v1_3_clean USING gist (geom);


-- set geom
UPDATE  sandbox.bnetza_mastr_solar_v1_3_clean
    SET geom = ST_TRANSFORM(ST_SetSRID(ST_Point(NULLIF("Laengengrad", '')::double precision, NULLIF("Breitengrad", '')::double precision),4326),4326),
        comment = COALESCE(comment, '') || 'make_geom; '
    WHERE   "Laengengrad" IS NOT NULL OR
            "Breitengrad" IS NOT NULL;


/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM sandbox.bnetza_mastr_solar_v1_3_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom) AS geom
FROM sandbox.bnetza_mastr_solar_v1_3_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY comment;
*/


-- Check geom
UPDATE  sandbox.bnetza_mastr_solar_v1_3_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'has_geom; '
    WHERE   geom IS NOT NULL;

-- Punkte innerhalb vg250
    UPDATE  sandbox.bnetza_mastr_solar_v1_3_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'inside_vg250; '
    FROM    (
        SELECT  m.id AS id
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                sandbox.bnetza_mastr_solar_v1_3_clean AS m
        WHERE   vg.geom && ST_TRANSFORM(m.geom,3035) AND
                ST_CONTAINS(vg.geom,ST_TRANSFORM(m.geom,3035))
        ) AS t2
    WHERE   t1.id = t2.id;

-- Punkte außerhalb
    UPDATE  sandbox.bnetza_mastr_solar_v1_3_clean
    SET     comment =  COALESCE(comment, '') || 'outside_vg250; '
    WHERE   comment = 'make_geom; has_geom; ';

-- Reset außerhalb Onshore 
    UPDATE  sandbox.bnetza_mastr_solar_v1_3_clean
    SET     geom =  NULL,
            comment =  COALESCE(comment, '') || 'remove_geom; '
    WHERE   comment = 'make_geom; has_geom; outside_vg250; ';


-- Make geom from PLZ
UPDATE  sandbox.bnetza_mastr_solar_v1_3_clean AS t1
    SET     geom = t2.geom,
            comment =  COALESCE(comment, '') || 'make_geom_plz; '
    FROM    (SELECT plz,
            ST_CENTROID(geom) ::geometry(Point,4326) AS geom
            FROM    boundaries.osm_postcode
            WHERE   stellen = 5
            ORDER BY plz
            )AS t2
    WHERE   t1."Postleitzahl" = t2.plz AND
            t1.geom IS NULL;


-- Analyze Solar
SELECT  "AnzahlModule", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY "AnzahlModule"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Lage", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY "Lage"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Hauptausrichtung", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY "Hauptausrichtung"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Nebenausrichtung", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY "Nebenausrichtung"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "InAnspruchGenommeneFlaeche", "ArtDerFlaeche", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY "InAnspruchGenommeneFlaeche", "ArtDerFlaeche"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Nutzungsbereich", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_solar_v1_3_clean
GROUP BY "Nutzungsbereich"
ORDER BY COUNT(*) DESC;
