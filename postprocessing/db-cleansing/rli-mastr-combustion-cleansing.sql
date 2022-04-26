/*
open_FRED MaStR Combustion Cleansing v2

Create a copy
Reset geom if outside of Germany
Allocate to centre of PLZ-area

__copyright__   = "© Reiner Lemoine Institut"
__license__ = "Creative Commons Zero v1.0 Universal (CC0-1.0)"
__url__     = "https://creativecommons.org/publicdomain/zero/1.0/"
__author__  = "Ludwig Hülk"
*/

ALTER TABLE model_draft.bnetza_mastr_combustion_clean
    ADD COLUMN id SERIAL;

ALTER TABLE model_draft.bnetza_mastr_combustion_clean
    ALTER COLUMN "Bruttoleistung" TYPE double precision USING "Bruttoleistung"::double precision;

CREATE INDEX bnetza_mastr_combustion_clean_geom_idx
    ON model_draft.bnetza_mastr_combustion_clean USING gist (geom);

/*
ALTER TABLE model_draft.bnetza_mastr_combustion_clean
    OWNER to oeuser;
*/

/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM model_draft.bnetza_mastr_combustion_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom) AS geom
FROM model_draft.bnetza_mastr_combustion_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM model_draft.bnetza_mastr_combustion_clean
GROUP BY comment;
*/


-- Check geom
UPDATE  model_draft.bnetza_mastr_combustion_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'has_geom; '
    WHERE   geom IS NOT NULL;

    UPDATE  model_draft.bnetza_mastr_combustion_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'no_geom; '
    WHERE   geom IS NULL;

-- Punkte innerhalb vg250
UPDATE  model_draft.bnetza_mastr_combustion_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'inside_vg250; '
    FROM    (
        SELECT  m.id AS id
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                model_draft.bnetza_mastr_combustion_clean AS m
        WHERE   m.geom && ST_TRANSFORM(vg.geom,4326) AND
                ST_CONTAINS(ST_TRANSFORM(vg.geom,4326),m.geom)
        ) AS t2
    WHERE   t1.id = t2.id;


-- Punkte außerhalb
UPDATE  model_draft.bnetza_mastr_combustion_clean
    SET     comment =  COALESCE(comment, '') || 'outside_vg250; '
    WHERE   comment = 'make_geom; has_geom; ';

-- Reset außerhalb
UPDATE  model_draft.bnetza_mastr_combustion_clean
    SET     geom =  NULL,
            comment =  COALESCE(comment, '') || 'remove_geom; '
    WHERE   comment = 'make_geom; has_geom; outside_vg250; ';


-- Make geom from PLZ
UPDATE  model_draft.bnetza_mastr_combustion_clean AS t1
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


-- Make geom from Standort (Extract PLZ)
UPDATE  model_draft.bnetza_mastr_combustion_clean AS t1
    SET     geom = t2.geom,
            comment =  COALESCE(comment, '') || 'make_geom_standort_plz; '
    FROM    (SELECT plz,
            ST_CENTROID(geom) ::geometry(Point,4326) AS geom
            FROM    boundaries.osm_postcode
            WHERE   stellen = 5
            ORDER BY plz
            ) AS t2,
            (SELECT "Standort", NULLIF(regexp_replace("Standort", '\D','','g'), '')::text AS plz
            FROM model_draft.bnetza_mastr_combustion_clean
            WHERE geom IS NULL
            ) AS t3
    WHERE   length(t3.plz) = 5 AND
            t3.plz = t2.plz::text AND
            t1.geom IS NULL;
-- ToDO: Use regexp_split_to_array to split words and remove single numbers


DELETE FROM model_draft.bnetza_mastr_combustion_clean
WHERE geom IS NULL;


----------------ALT
SELECT *
FROM model_draft.bnetza_mastr_combustion_clean
WHERE comment = 'make_geom_standort_plz; ';

-- Check geom
SELECT *
FROM model_draft.bnetza_mastr_combustion_clean
WHERE geom IS NULL;

-- Analyze Wind
SELECT  "Technologie", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_combustion_clean
GROUP BY "Technologie";

-- Create a reduced version
DROP TABLE IF EXISTS model_draft.bnetza_mastr_combustion_clean_reduced CASCADE;
CREATE TABLE         model_draft.bnetza_mastr_combustion_clean_reduced AS
    SELECT  "EinheitMastrNummer",
            "Bruttoleistung",
            "EinheitBetriebsstatus",
            "Anlagenbetreiber",
            "EegMastrNummer",
            "KwkMastrNummer",
            "GenMastrNummer",
            "DatumLetzteAktualisierung",
            "LokationMastrNummer",
            "NetzbetreiberpruefungStatus",
            "NetzbetreiberpruefungDatum",
            "AnlagenbetreiberMastrNummer",
            "Land",
            "Bundesland",
            "Landkreis",
            "Gemeinde",
            "Gemeindeschluessel",
            "Postleitzahl",
            "Gemarkung",
            "FlurFlurstuecknummern",
            "Strasse",
            "StrasseNichtGefunden",
            "Hausnummer",
            "HausnummerNichtGefunden",
            "Adresszusatz",
            "Ort",
            "Laengengrad",
            "Breitengrad",
            "Meldedatum",
            "Inbetriebnahmedatum",
            "DatumEndgueltigeStilllegung",
            "DatumBeginnVoruebergehendeStilllegung",
            "DatumWiederaufnahmeBetrieb",
            "EinheitBetriebsstatus_extended",
            "AltAnlagenbetreiberMastrNummer",
            "DatumDesBetreiberwechsels",
            "DatumRegistrierungDesBetreiberwechsels",
            "StatisikFlag_basic",
            "NameStromerzeugungseinheit",
            "WeicDisplayName",
            "Bruttoleistung_extended",
            "Nettonennleistung",
            "AnschlussAnHoechstOderHochSpannung",
            "FernsteuerbarkeitNb",
            "FernsteuerbarkeitDv",
            "FernsteuerbarkeitDr",
            "Einspeisungsart",
            "GenMastrNummer_extended",
            "Meldedatum_kwk",
            "DatumLetzteAktualisierung_kwk",
            "geom",
            "comment"
    FROM    model_draft.bnetza_mastr_combustion_clean
    WHERE geom IS NOT NULL;

CREATE INDEX bnetza_mastr_combustion_clean_reduced_geom_idx
    ON model_draft.bnetza_mastr_combustion_clean_reduced USING gist (geom);

