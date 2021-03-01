/*
RLI MaStR Wind Cleansing

Create a copy
Create the geom from coordinates
Check if geom is inside Germany
Reset geom if outside of Germany (and not Wind Offshore)
Allocate to centre of PLZ-area

__copyright__   = "© Reiner Lemoine Institut"
__license__ = "Creative Commons Zero v1.0 Universal (CC0-1.0)"
__url__     = "https://creativecommons.org/publicdomain/zero/1.0/"
__author__  = "Ludwig Hülk"
*/

ALTER TABLE model_draft.bnetza_mastr_wind_clean
    ADD COLUMN id SERIAL;

ALTER TABLE model_draft.bnetza_mastr_wind_clean
    ALTER COLUMN "Bruttoleistung" TYPE double precision USING "Bruttoleistung"::double precision,
    ALTER COLUMN "Bruttoleistung_extended" TYPE double precision USING "Bruttoleistung_extended"::double precision,
    ALTER COLUMN "Nettonennleistung" TYPE double precision USING "Nettonennleistung"::double precision,
    ALTER COLUMN "Nabenhoehe" TYPE double precision USING "Nabenhoehe"::double precision,
    ALTER COLUMN "Rotordurchmesser" TYPE double precision USING "Rotordurchmesser"::double precision,
    ALTER COLUMN "Wassertiefe" TYPE double precision USING "Wassertiefe"::double precision,
    ALTER COLUMN "Kuestenentfernung" TYPE double precision USING "Kuestenentfernung"::double precision
    ;

CREATE INDEX bnetza_mastr_wind_clean_geom_3035_idx
    ON model_draft.bnetza_mastr_wind_clean USING gist (geom_3035);

CREATE INDEX bnetza_mastr_wind_clean_geom_idx
    ON model_draft.bnetza_mastr_wind_clean USING gist (geom);


-- Tags: Onshore and offshore
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = '{"processed": true}';


-- Tags: Onshore and offshore
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"location":"onshore"}'
    WHERE   "Lage" = 'WindAnLand';

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"location":"offshore"}'
    WHERE   "Lage" = 'WindAufSee';

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"location":"na"}'
    WHERE   "Lage" IS NULL;


-- Tags: Migrierte Einheiten
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"einheit":"SME"}'
    WHERE LEFT("EinheitMastrNummer", 3) = 'SME';

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"einheit":"SEE"}'
    WHERE LEFT("EinheitMastrNummer", 3) = 'SEE';


-- Tags: Gematchte Einheiten
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"flag":"A"}'
    WHERE "StatisikFlag" = 'A';

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"flag":"B"}'
    WHERE "StatisikFlag" = 'B';


-- Tags: Betriebsstatus
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"status":"operating"}'
    WHERE "EinheitBetriebsstatus" = 'InBetrieb';

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"status":"planned"}'
    WHERE "EinheitBetriebsstatus" = 'InPlanung';

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"status":"shutdown"}'
    WHERE   "EinheitBetriebsstatus" = 'DauerhaftStillgelegt' OR
            "EinheitBetriebsstatus" = 'VoruebergehendStillgelegt';


-- Check capacity
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET     tags = tags || '{"capacity":"reduce/10"}',
            "Bruttoleistung" =  "Bruttoleistung" / 10
    WHERE   "Bruttoleistung" > 9500;

-- Tags: geom
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"geom": false}'
    WHERE   geom IS NULL;


-- Check if geom is inside Germany (vg250)
UPDATE  model_draft.bnetza_mastr_wind_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'onshore_inside; ',
            tags = tags || '{"inside_germany": true}'
    FROM    (
        SELECT  m.id AS id
        FROM    boundaries.bkg_vg250_1_sta_union_mview AS vg,
                model_draft.bnetza_mastr_wind_clean AS m
        WHERE   tags ->> 'location' = 'onshore' AND
                ST_TRANSFORM(vg.geom, 4326) && m.geom AND
                ST_CONTAINS(ST_TRANSFORM(vg.geom, 4326),m.geom)
        ) AS t2
    WHERE   t1.id = t2.id;

-- Tags: geom outside Germany
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"inside_germany": false}'
    WHERE NOT tags ? 'inside_germany';


-- Remove geom onshore outside
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET     geom =  NULL,
            tags = tags || '{"geom": false, "geom_remove": true}'
    WHERE   (tags->>'geom')::boolean IS true AND
    tags ->> 'location' = 'onshore'  AND
    (tags->>'inside_germany')::boolean IS false;

-- Transform geom
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET     geom_3035 =  ST_TRANSFORM(geom, 3035)
    WHERE   geom IS NOT NULL;


-- Make geom from PLZ (ST_PointOnSurface)
UPDATE  model_draft.bnetza_mastr_wind_clean AS t1
    SET     geom_3035 = t2.geom,
            comment =  COALESCE(comment, '') || 'onshore_fix_plz; ',
            tags = tags || '{"geom": true, "inside_germany": true,"geom_guess":"plz"}'
    FROM    (SELECT plz,
            ST_PointOnSurface(ST_TRANSFORM(ST_MakeValid(geom),3035)) ::geometry(Point,3035) AS geom
            FROM    boundaries.osm_postcode
            WHERE   stellen = 5
            ORDER BY plz
            )AS t2
    WHERE   t1."Postleitzahl" = t2.plz AND
            t1.geom_3035 IS NULL AND
            t1.tags ->> 'location' = 'onshore';


-- Make geom from Standort (Extract PLZ)
UPDATE  model_draft.bnetza_mastr_wind_clean AS t1
    SET     geom_3035 = t2.geom,
            comment =  COALESCE(comment, '') || 'onshore_fix_plz2; ',
            tags = tags || '{"geom": true, "inside_germany": true,"geom_guess":"standort_plz"}'
    FROM    (SELECT plz,
            ST_PointOnSurface(ST_TRANSFORM(ST_MakeValid(geom),3035)) ::geometry(Point,3035) AS geom
            FROM    boundaries.osm_postcode
            WHERE   stellen = 5
            ORDER BY plz
            ) AS t2,
            (SELECT "Standort", NULLIF(regexp_replace("Standort", '\D','','g'), '') AS plz
            FROM model_draft.bnetza_mastr_wind_clean
            WHERE geom_3035 IS NULL
            ) AS t3
    WHERE   length(t3.plz) = 5 AND
            t3.plz = t2.plz::text AND
            t1.geom_3035 IS NULL AND
            t1.tags ->> 'location' = 'onshore';
-- ToDO: Use regexp_split_to_array to split words and remove single numbers




-- Check Offshore
    UPDATE  model_draft.bnetza_mastr_wind_clean AS t1
    SET     comment =  COALESCE(comment, '') || 'offshore_inside; ',
            tags = tags || '{"inside_germany": true}'
    FROM    (
        SELECT  m.id AS id
        FROM    model_draft.rli_boundaries_offshore AS vg,
                model_draft.bnetza_mastr_wind_clean AS m
        WHERE   m."Lage" = 'WindAufSee' AND
                m.geom_3035 && vg.geom AND
                ST_CONTAINS(vg.geom,m.geom_3035)
        ) AS t2
    WHERE   t1.id = t2.id;

-- Remove geom offshore outside
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET     geom_3035 =  NULL,
            tags = tags || '{"geom": false, "geom_remove": true}'
    WHERE   (tags->>'geom')::boolean IS true AND
    tags ->> 'location' = 'offshore'  AND
    (tags->>'inside_germany')::boolean IS false;



-- Manual set geom for NULL
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET comment =  COALESCE(comment, '') || 'guess_geom; ',
        tags = tags || '{"geom": true, "inside_germany": true,"geom_guess":"somewhere"}',
        geom_3035 = ST_TRANSFORM(ST_SetSRID(ST_Point(
                                                8.0,
                                                54.0)
                                                ,4326),3035)
    WHERE   geom_3035 IS NULL;


-- Check all PLZ
UPDATE  model_draft.bnetza_mastr_wind_clean AS t1
    SET     tags = tags || '{"plz_check": true}'
    FROM    (
        SELECT  m.id AS id
        FROM    boundaries.osm_postcode AS plz,
                model_draft.bnetza_mastr_wind_clean AS m
        WHERE   ST_TRANSFORM(plz.geom, 3035) && m.geom_3035 AND
                ST_CONTAINS(ST_TRANSFORM(plz.geom, 3035),m.geom_3035) AND
                m."Postleitzahl" = plz.plz AND
                m.tags ->> 'location' = 'onshore'
        ) AS t2
    WHERE   t1.id = t2.id;

UPDATE  model_draft.bnetza_mastr_wind_clean
    SET tags = tags || '{"plz_check": false}'
    WHERE NOT tags ? 'plz_check';

---------------------

-- Check for matching



-- OSM power buffer
DROP TABLE IF EXISTS    model_draft.mastr_osm_deu_point_windpower_buffer CASCADE;
CREATE TABLE            model_draft.mastr_osm_deu_point_windpower_buffer (
    id SERIAL,
    osm_name    text,
    osm_count   double precision,
    osm_sum     double precision,
    mastr_count double precision,
    mastr_sum   double precision,
    geom geometry(Polygon,3035),
    CONSTRAINT osm_deu_point_windpower_buffer_pkey PRIMARY KEY (id));


-- SELECT ST_SetSRID(geom, 3035) FROM model_draft.mastr_osm_deu_point_windpower;
SELECT UpdateGeometrySRID('model_draft', 'mastr_osm_deu_point_windpower', 'geom',  3035);

-- insert buffer
INSERT INTO model_draft.mastr_osm_deu_point_windpower_buffer (geom)
    SELECT  (ST_DUMP(ST_MULTI(ST_UNION(
            ST_BUFFER(geom, 25)
        )))).geom ::geometry(Polygon,3035) AS geom
    FROM    model_draft.mastr_osm_deu_point_windpower;

CREATE INDEX mastr_osm_deu_point_windpower_buffer_geom_idx
    ON model_draft.mastr_osm_deu_point_windpower_buffer USING gist (geom);


-- mastr in OSM buffer
UPDATE model_draft.mastr_osm_deu_point_windpower_buffer AS t1
    SET mastr_sum = t2.mastr_sum,
        mastr_count = t2.mastr_count
    FROM (
        SELECT  a.id AS id,
                SUM(b."Bruttoleistung") AS mastr_sum,
                COUNT(b.geom_3035)::integer AS mastr_count
        FROM    model_draft.mastr_osm_deu_point_windpower_buffer AS a,
                model_draft.bnetza_mastr_wind_clean AS b
        WHERE   a.geom && b.geom_3035 AND
                ST_CONTAINS(a.geom,b.geom_3035)
        GROUP BY a.id
        )AS t2
    WHERE   t1.id = t2.id;


-- OSM in OSM
UPDATE model_draft.mastr_osm_deu_point_windpower_buffer AS t1
    SET osm_count = t2.osm_count
    FROM (
        SELECT  a.id AS id,
                COUNT(b.geom)::integer AS osm_count
        FROM    model_draft.mastr_osm_deu_point_windpower_buffer AS a,
                model_draft.mastr_osm_deu_point_windpower AS b
        WHERE   a.geom && b.geom AND
                ST_CONTAINS(a.geom,b.geom)
        GROUP BY a.id
        )AS t2
    WHERE   t1.id = t2.id;


-- OSM_id -> mastr
ALTER TABLE model_draft.bnetza_mastr_wind_clean
    ADD COLUMN osm_id integer,
    ADD COLUMN osm_name text;

UPDATE model_draft.bnetza_mastr_wind_clean AS t1
    SET     osm_id = t2.osm_id,
            osm_name = t2.osm_name,
            tags = tags || '{"in_osm": true}'
    FROM (
        SELECT  b.id AS id,
                a.id as osm_id,
                a.osm_name as osm_name
        FROM    model_draft.mastr_osm_deu_point_windpower_buffer AS a,
                model_draft.bnetza_mastr_wind_clean AS b
        WHERE   a.geom && b.geom_3035 AND
                ST_CONTAINS(a.geom,b.geom_3035)
        GROUP BY b.id, a.id
        )AS t2
    WHERE   t1.id = t2.id;



-- MaStR buffer
DROP TABLE IF EXISTS    model_draft.bnetza_mastr_wind_clean_buffer CASCADE;
CREATE TABLE            model_draft.bnetza_mastr_wind_clean_buffer (
    id SERIAL,
    osm_name    text,
    osm_count   double precision,
    osm_sum     double precision,
    mastr_count double precision,
    mastr_sum   double precision,
    geom geometry(Polygon,3035),
    CONSTRAINT bnetza_mastr_wind_clean_buffer_pkey PRIMARY KEY (id));


-- insert buffer
INSERT INTO model_draft.bnetza_mastr_wind_clean_buffer (geom)
    SELECT  (ST_DUMP(ST_MULTI(ST_UNION(
            ST_BUFFER(geom_3035, 25)
        )))).geom ::geometry(Polygon,3035) AS geom
    FROM    model_draft.bnetza_mastr_wind_clean;

CREATE INDEX bnetza_mastr_wind_clean_buffer_geom_idx
    ON model_draft.bnetza_mastr_wind_clean_buffer USING gist (geom);


-- mastr in mastr buffer
UPDATE model_draft.bnetza_mastr_wind_clean_buffer AS t1
    SET mastr_sum = t2.mastr_sum,
        mastr_count = t2.mastr_count
    FROM (
        SELECT  a.id AS id,
                SUM(b."Bruttoleistung") AS mastr_sum,
                COUNT(b.geom_3035)::integer AS mastr_count
        FROM    model_draft.bnetza_mastr_wind_clean_buffer AS a,
                model_draft.bnetza_mastr_wind_clean AS b
        WHERE   a.geom && b.geom_3035 AND
                ST_CONTAINS(a.geom,b.geom_3035)
        GROUP BY a.id
        )AS t2
    WHERE   t1.id = t2.id;

-- OSM in mastr buffer
UPDATE model_draft.bnetza_mastr_wind_clean_buffer AS t1
    SET osm_count = t2.osm_count
    FROM (
        SELECT  a.id AS id,
                COUNT(b.geom)::integer AS osm_count
        FROM    model_draft.bnetza_mastr_wind_clean_buffer AS a,
                model_draft.mastr_osm_deu_point_windpower AS b
        WHERE   a.geom && b.geom AND
                ST_CONTAINS(a.geom,b.geom)
        GROUP BY a.id
        )AS t2
    WHERE   t1.id = t2.id;



-- MaStR-Buffer -> mastr
ALTER TABLE model_draft.bnetza_mastr_wind_clean
    ADD COLUMN mastr_buffer_cnt integer;

UPDATE model_draft.bnetza_mastr_wind_clean AS t1
    SET     mastr_buffer_cnt = t2.mastr_buffer_cnt
    FROM (
        SELECT  b.id AS id,
                a.mastr_count as mastr_buffer_cnt
        FROM    model_draft.bnetza_mastr_wind_clean_buffer AS a,
                model_draft.bnetza_mastr_wind_clean AS b
        WHERE   a.geom && b.geom_3035 AND
                ST_CONTAINS(a.geom,b.geom_3035)
        GROUP BY b.id, a.id
        )AS t2
    WHERE   t1.id = t2.id;




-- Tags: duplicates
UPDATE  model_draft.bnetza_mastr_wind_clean
    SET comment =  COALESCE(comment, '') || 'match_osm; ',
        tags = tags || '{"match_osm": true}'
    WHERE   (tags->>'in_osm')::boolean IS true;




-- Create
DROP TABLE IF EXISTS model_draft.bnetza_mastr_wind_clean_flagb CASCADE;
CREATE TABLE         model_draft.bnetza_mastr_wind_clean_flagb AS
    SELECT  *
    FROM    model_draft.bnetza_mastr_wind_clean
    WHERE   "StatisikFlag" = 'B' AND
            "EinheitBetriebsstatus" = 'InBetrieb'
    ORDER BY id;















-------------------- checks

--UPDATE  model_draft.bnetza_mastr_wind_clean
--    SET tags = tags || '{"geom": true, "inside_germany": false}'

-- Select JSONB
SELECT tags, count(tags) AS count
FROM model_draft.bnetza_mastr_wind_clean
GROUP BY tags
ORDER BY count DESC;

-- Count locations
SELECT tags ->> 'location' AS locations, count(tags ->> 'location') AS count
FROM model_draft.bnetza_mastr_wind_clean
GROUP BY locations
ORDER BY count DESC;

SELECT count(*)
FROM model_draft.bnetza_mastr_wind_clean
WHERE tags ->> 'location' = 'offshore';

SELECT (tags->>'geom')::boolean as geom_type, count(*)
FROM model_draft.bnetza_mastr_wind_clean
WHERE (tags->>'geom')::boolean
GROUP BY geom_type;

SELECT (tags->>'geom')::boolean as geom_type, count(*)
FROM model_draft.bnetza_mastr_wind_clean
WHERE (tags->>'geom')::boolean IS false
GROUP BY geom_type;

-- Check geom
SELECT *
FROM model_draft.bnetza_mastr_wind_clean
WHERE geom IS NULL;

-- Analyze Wind
SELECT  "Technologie", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_wind_clean
GROUP BY "Technologie";

-- Analyze Wind
SELECT  "HerstellerId", "Hersteller", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_wind_clean
GROUP BY "HerstellerId","Hersteller"
ORDER BY COUNT(*) DESC;

-- Analyze Wind
SELECT  "HerstellerId", "Hersteller", "Typenbezeichnung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_wind_clean
GROUP BY "HerstellerId","Hersteller", "Typenbezeichnung"
ORDER BY COUNT(*) DESC;

/*
-- Analyze Wind
SELECT  'ALL' AS "Typenbezeichnung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_wind_clean_50hertz
WHERE is_50hertz = TRUE
UNION ALL
SELECT  "Typenbezeichnung", COUNT(*) AS cnt
FROM model_draft.bnetza_mastr_wind_clean_50hertz
WHERE is_50hertz = TRUE
GROUP BY "Typenbezeichnung"
*/


/*
-- TEST
SELECT ST_SetSRID(ST_Point("Laengengrad", "Breitengrad"),4326) As wgs84long_lat
FROM model_draft.bnetza_mastr_wind_clean
LIMIT 10;

SELECT  COUNT(*) AS all,
        COUNT("Laengengrad") AS lon,
        COUNT("Breitengrad") AS lat,
        COUNT(geom_3035) AS geom
FROM model_draft.bnetza_mastr_wind_clean;

SELECT  comment, COUNT(comment) AS cnt
FROM model_draft.bnetza_mastr_wind_clean
GROUP BY comment;
*/

-------------------------------------------------------
-- Create a reduced version
DROP TABLE IF EXISTS model_draft.bnetza_mastr_wind_clean_reduced CASCADE;
CREATE TABLE         model_draft.bnetza_mastr_wind_clean_reduced AS
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
            "StatisikFlag",
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
            "NameWindpark",
            "Lage",
            "Seelage",
            "ClusterOstsee",
            "ClusterNordsee",
            "Technologie",
            "Typenbezeichnung",
            "Nabenhoehe",
            "Rotordurchmesser",
            "AuflageAbschaltungLeistungsbegrenzung",
            "Wassertiefe",
            "Kuestenentfernung",
            "EegMastrNummer_extended",
            "HerstellerId",
            "Hersteller",
            "Meldedatum_eeg",
            "DatumLetzteAktualisierung_eeg",
            "EegInbetriebnahmedatum",
            "AnlagenkennzifferAnlagenregister",
            "AnlagenschluesselEeg",
            "InstallierteLeistung",
            "VerhaeltnisErtragsschaetzungReferenzertrag",
            "AnlageBetriebsstatus",
            "Datum",
            "Art",
            "Behoerde",
            "Aktenzeichen",
            "Frist",
            "Meldedatum_permit",
            "geom"
    FROM    model_draft.bnetza_mastr_wind_clean
    WHERE geom IS NOT NULL;

ALTER TABLE model_draft.bnetza_mastr_wind_clean_reduced
    ADD PRIMARY KEY ("EinheitMastrNummer");

CREATE INDEX bnetza_mastr_wind_clean_reduced_geom_idx
    ON model_draft.bnetza_mastr_wind_clean_reduced USING gist (geom);
