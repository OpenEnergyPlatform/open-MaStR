/*
open_FRED MaStR Wind Cleansing

Create a copy
Reset geom if outside of Germany (and not Wind Offshore)
Allocate to centre of PLZ-area

__copyright__   = "© Reiner Lemoine Institut"
__license__ = "Creative Commons Zero v1.0 Universal (CC0-1.0)"
__url__     = "https://creativecommons.org/publicdomain/zero/1.0/"
__author__  = "Ludwig Hülk"
*/

-- Create a copy
DROP TABLE IF EXISTS sandbox.bnetza_mastr_wind_v1_4_clean CASCADE;
CREATE TABLE         sandbox.bnetza_mastr_wind_v1_4_clean AS
    SELECT  * 
    FROM    sandbox."bnetza_mastr_wind_rli_v1.4"
    ORDER BY index;

ALTER TABLE sandbox.bnetza_mastr_wind_v1_4_clean
    ADD PRIMARY KEY (index),
    ADD COLUMN "comment" text;

CREATE INDEX bnetza_mastr_wind_v1_4_clean_geom_idx
    ON sandbox.bnetza_mastr_wind_v1_4_clean USING gist (geom);


-- set geom
UPDATE  sandbox.bnetza_mastr_wind_v1_4_clean
    SET geom = ST_TRANSFORM(ST_SetSRID(ST_Point("Laengengrad"::double precision, "Breitengrad"::double precision),4326),4326),
        comment = COALESCE(comment, '') || 'make_geom; '
    WHERE   "Laengengrad" IS NOT NULL OR
            "Breitengrad" IS NOT NULL;

/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM sandbox.bnetza_mastr_wind_v1_4_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom) AS geom
FROM sandbox.bnetza_mastr_wind_v1_4_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM sandbox.bnetza_mastr_wind_v1_4_clean
GROUP BY comment;
*/


-- Check geom
UPDATE  sandbox.bnetza_mastr_wind_v1_4_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'has_geom; '
    WHERE   geom IS NOT NULL

-- Punkte innerhalb vg250
    UPDATE  sandbox.bnetza_mastr_wind_v1_4_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'inside_vg250; '
    FROM    (
        SELECT  m.index AS index
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                sandbox.bnetza_mastr_wind_v1_4_clean AS m
        WHERE   vg.geom && ST_TRANSFORM(m.geom,3035) AND
                ST_CONTAINS(vg.geom,ST_TRANSFORM(m.geom,3035))
        ) AS t2
    WHERE   t1.index = t2.index;

-- Punkte außerhalb nicht Offshore
    UPDATE  sandbox.bnetza_mastr_wind_v1_4_clean
    SET     comment =  COALESCE(comment, '') || 'outside_vg250_onshore; '
    WHERE   comment = 'has_geom; ' 
            AND "Lage" = 'WindAnLand';

-- Reset außerhalb Onshore 
    UPDATE  sandbox.bnetza_mastr_wind_v1_4_clean
    SET     geom =  NULL,
            comment =  COALESCE(comment, '') || 'remove_geom; '
    WHERE   comment = 'has_geom; outside_vg250_onshore; ';


-- Make geom from PLZ
UPDATE  sandbox.bnetza_mastr_wind_v1_4_clean AS t1
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


-- Analyze Wind
SELECT  "Technologie", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_wind_v1_4_clean
GROUP BY "Technologie"

-- Analyze Wind
SELECT  "HerstellerID", "HerstellerName", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_wind_v1_4_clean
GROUP BY "HerstellerID","HerstellerName"
ORDER BY COUNT(*) DESC;

-- Analyze Wind
SELECT  "HerstellerID", "HerstellerName", "Typenbezeichnung", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_wind_v1_4_clean
GROUP BY "HerstellerID","HerstellerName", "Typenbezeichnung"
ORDER BY COUNT(*) DESC;

-- Analyze Wind
SELECT  'ALL' AS "Typenbezeichnung", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_wind_v1_4_clean_50hertz
WHERE is_50hertz = TRUE
UNION ALL
SELECT  "Typenbezeichnung", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_wind_v1_4_clean_50hertz
WHERE is_50hertz = TRUE
GROUP BY "Typenbezeichnung"