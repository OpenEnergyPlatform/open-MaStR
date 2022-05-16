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

ALTER TABLE model_draft.bnetza_mastr_solar_clean
    ADD COLUMN id SERIAL;

ALTER TABLE model_draft.bnetza_mastr_solar_clean
    ALTER COLUMN "Bruttoleistung" TYPE double precision USING "Bruttoleistung"::double precision;

CREATE INDEX bnetza_mastr_solar_clean_geom_idx
    ON model_draft.bnetza_mastr_solar_clean USING gist (geom);

/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM model_draft.bnetza_mastr_solar_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom) AS geom
FROM model_draft.bnetza_mastr_solar_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY comment;
*/


-- Check geom
UPDATE  model_draft.bnetza_mastr_solar_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'has_geom; '
    WHERE   geom IS NOT NULL;

-- Punkte innerhalb vg250
    UPDATE  model_draft.bnetza_mastr_solar_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'inside_vg250; '
    FROM    (
        SELECT  m.id AS id
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                model_draft.bnetza_mastr_solar_clean AS m
        --WHERE   vg.geom && ST_TRANSFORM(m.geom,3035) AND
                --ST_CONTAINS(vg.geom,ST_TRANSFORM(m.geom,3035))
		WHERE   vg.geom && m.geom AND
		ST_CONTAINS(vg.geom, m.geom)
        ) AS t2
    WHERE   t1.id = t2.id;


-- Punkte außerhalb
    UPDATE  model_draft.bnetza_mastr_solar_clean
    SET     comment =  COALESCE(comment, '') || 'outside_vg250; '
    WHERE   comment = 'make_geom; has_geom; ';

-- Reset außerhalb Onshore
    UPDATE  model_draft.bnetza_mastr_solar_clean
    SET     geom =  NULL,
            comment =  COALESCE(comment, '') || 'remove_geom; '
    WHERE   comment = 'make_geom; has_geom; outside_vg250; ';


-- Make geom from PLZ
UPDATE  model_draft.bnetza_mastr_solar_clean AS t1
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
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY "AnzahlModule"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Lage", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY "Lage"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Hauptausrichtung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY "Hauptausrichtung"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Nebenausrichtung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY "Nebenausrichtung"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "InAnspruchGenommeneFlaeche", "ArtDerFlaeche", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY "InAnspruchGenommeneFlaeche", "ArtDerFlaeche"
ORDER BY COUNT(*) DESC;

-- Analyze Solar
SELECT  "Nutzungsbereich", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_solar_clean
GROUP BY "Nutzungsbereich"
ORDER BY COUNT(*) DESC;
