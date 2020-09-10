/*
open_FRED MaStR Hydro Cleansing v2

Create a copy
Reset geom if outside of Germany
Allocate to centre of PLZ-area

__copyright__   = "© Reiner Lemoine Institut"
__license__ = "Creative Commons Zero v1.0 Universal (CC0-1.0)"
__url__     = "https://creativecommons.org/publicdomain/zero/1.0/"
__author__  = "Ludwig Hülk"
*/

-- Create a copy
DROP TABLE IF EXISTS model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean CASCADE;
CREATE TABLE         model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean AS
    SELECT  * 
    FROM    model_draft.bnetza_mastr_rli_v2_4_1_hydro
    ORDER BY id;

ALTER TABLE model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    ADD PRIMARY KEY (id),
    ADD COLUMN lat double precision,
    ADD COLUMN lon double precision,
    ADD COLUMN "geom" geometry(Point,4326),
    ADD COLUMN "comment" text;

ALTER TABLE model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    ALTER COLUMN "Bruttoleistung" TYPE double precision USING "Bruttoleistung"::double precision;

CREATE INDEX bnetza_mastr_rli_v2_4_1_hydro_clean_geom_idx
    ON model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean USING gist (geom);

-- clean lat and lon values
UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    SET lat =      CAST(
                        COALESCE(
                            NULLIF(
                                regexp_replace("Breitengrad", '[^-0-9.]+', '', 'g'), 
                                ''),
                            NULL) 
                       AS numeric),
       lon =      CAST(
                        COALESCE(
                            NULLIF(
                                regexp_replace("Laengengrad", '[^-0-9.]+', '', 'g'), 
                                ''),
                            NULL) 
                       AS numeric)
                       ;

-- set geom
UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    SET geom = ST_TRANSFORM(ST_SetSRID(ST_Point(
                                                lon,
                                                lat)
                                                ,4326),4326),
        comment = COALESCE(comment, '') || 'make_geom; '
    WHERE   lon IS NOT NULL AND
            lat IS NOT NULL;

/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom) AS geom
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
GROUP BY comment;
*/


-- Check geom
UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'has_geom; '
    WHERE   geom IS NOT NULL;

-- Punkte innerhalb vg250
    UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'inside_vg250; '
    FROM    (
        SELECT  m.id AS id
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean AS m
        WHERE   m.geom && ST_TRANSFORM(vg.geom,4326) AND
                ST_CONTAINS(ST_TRANSFORM(vg.geom,4326),m.geom)
        ) AS t2
    WHERE   t1.id = t2.id;


-- Punkte außerhalb
    UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    SET     comment =  COALESCE(comment, '') || 'outside_vg250; '
    WHERE   comment = 'make_geom; has_geom; ';

-- Reset außerhalb 
    UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    SET     geom =  NULL,
            comment =  COALESCE(comment, '') || 'remove_geom; '
    WHERE   comment = 'make_geom; has_geom; outside_vg250; ';


-- Make geom from PLZ
UPDATE  model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean AS t1
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

ALTER TABLE model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
    OWNER to oeuser;

/*
-- Analyze Wasser
SELECT  "Technologie", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
GROUP BY "Technologie";

-- Analyze Wasser
SELECT  "HerstellerID", "HerstellerName", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
GROUP BY "HerstellerID","HerstellerName"
ORDER BY COUNT(*) DESC;

-- Analyze Wasser
SELECT  "HerstellerID", "HerstellerName", "Typenbezeichnung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean
GROUP BY "HerstellerID","HerstellerName", "Typenbezeichnung"
ORDER BY COUNT(*) DESC;


-- Analyze Wasser
SELECT  'ALL' AS "Typenbezeichnung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean_50hertz
WHERE is_50hertz = TRUE
UNION ALL
SELECT  "Typenbezeichnung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_rli_v2_4_1_hydro_clean_50hertz
WHERE is_50hertz = TRUE
GROUP BY "Typenbezeichnung"
*/