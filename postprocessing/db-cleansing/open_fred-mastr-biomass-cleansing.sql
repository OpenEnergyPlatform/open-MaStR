/*
open_FRED MaStR Biomass Cleansing

Create a copy
Reset geom if outside of Germany
Allocate to centre of PLZ-area

__copyright__   = "© Reiner Lemoine Institut"
__license__ = "Creative Commons Zero v1.0 Universal (CC0-1.0)"
__url__     = "https://creativecommons.org/publicdomain/zero/1.0/"
__author__  = "Ludwig Hülk"
*/

-- Create a copy
DROP TABLE IF EXISTS sandbox.bnetza_mastr_biomass_v1_4_clean CASCADE;
CREATE TABLE         sandbox.bnetza_mastr_biomass_v1_4_clean AS
    SELECT  * 
    FROM    sandbox.bnetza_mastr_biomass_v1_4
    ORDER BY vid;

ALTER TABLE sandbox.bnetza_mastr_biomass_v1_4_clean
    ADD PRIMARY KEY (vid),
    ADD COLUMN geom geometry(Point,4326),
    ADD COLUMN "comment" text;

CREATE INDEX bnetza_mastr_biomass_v1_4_clean_geom_idx
    ON sandbox.bnetza_mastr_biomass_v1_4_clean USING gist (geom);


-- set geom
UPDATE  sandbox.bnetza_mastr_biomass_v1_4_clean
    SET geom = ST_TRANSFORM(ST_SetSRID(ST_Point(NULLIF("Laengengrad", '')::double precision, NULLIF("Breitengrad", '')::double precision),4326),4326),
        comment = COALESCE(comment, '') || 'make_geom; '
    WHERE   "Laengengrad" IS NOT NULL OR
            "Breitengrad" IS NOT NULL;

/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM sandbox.bnetza_mastr_biomass_v1_4_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom) AS geom
FROM sandbox.bnetza_mastr_biomass_v1_4_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM sandbox.bnetza_mastr_biomass_v1_4_clean
GROUP BY comment;
*/


-- Check geom
UPDATE  sandbox.bnetza_mastr_biomass_v1_4_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'has_geom; '
    WHERE   geom IS NOT NULL;

-- Punkte innerhalb vg250
    UPDATE  sandbox.bnetza_mastr_biomass_v1_4_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'inside_vg250; '
    FROM    (
        SELECT  m.vid AS vid
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                sandbox.bnetza_mastr_biomass_v1_4_clean AS m
        WHERE   vg.geom && ST_TRANSFORM(m.geom,3035) AND
                ST_CONTAINS(vg.geom,ST_TRANSFORM(m.geom,3035))
        ) AS t2
    WHERE   t1.vid = t2.vid;

-- Punkte außerhalb
    UPDATE  sandbox.bnetza_mastr_biomass_v1_4_clean
    SET     comment =  COALESCE(comment, '') || 'outside_vg250; '
    WHERE   comment = 'make_geom; has_geom; ';

-- Reset außerhalb Onshore 
    UPDATE  sandbox.bnetza_mastr_biomass_v1_4_clean
    SET     geom =  NULL,
            comment =  COALESCE(comment, '') || 'remove_geom; '
    WHERE   comment = 'make_geom; has_geom; outside_vg250; ';


-- Make geom from PLZ
UPDATE  sandbox.bnetza_mastr_biomass_v1_4_clean AS t1
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


-- Analyze Biomass
SELECT  "Hauptbrennstoff", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_biomass_v1_4_clean
GROUP BY "Hauptbrennstoff"
ORDER BY COUNT(*) DESC;

-- Analyze Biomass
SELECT  "Biomasseart", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_biomass_v1_4_clean
GROUP BY "Biomasseart"
ORDER BY COUNT(*) DESC;

-- Analyze Biomass
SELECT  "Technologie", COUNT(*) AS cnt
FROM sandbox.bnetza_mastr_biomass_v1_4_clean
GROUP BY "Technologie"
ORDER BY COUNT(*) DESC;