"""Release test: download the full MaStR and check the resulting database.

This automates the slow test that is run manually before a release. It
downloads the complete MaStR bulk export and then runs a lot of hard-coded
consistency checks.

The download takes several hours. To run only the checks against a database
that was downloaded before, comment out the `download` call in `main`.
"""

import re

from sqlalchemy import event, inspect, text

from open_mastr import Mastr
from open_mastr.utils.sqlalchemy_tables import MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS
from open_mastr.utils.sqlalchemy_views import OLD_MASTR_TABLE_TO_OLD_DB_TABLE_NAME

# Every table that a full download must produce, with a conservative lower
# bound for its number of rows.
MINIMUM_ROW_COUNTS = {
    "AnlagenEegBiomasse": 10_000,
    "AnlagenEegGeothermieGrubengasDruckentspannung": 50,
    "AnlagenEegSolar": 2_000_000,
    "AnlagenEegSpeicher": 100_000,
    "AnlagenEegWasser": 3_000,
    "AnlagenEegWind": 15_000,
    "AnlagenGasSpeicher": 20,
    "AnlagenKwk": 10_000,
    "AnlagenStromSpeicher": 500_000,
    "Bilanzierungsgebiete": 100,
    "EinheitenAenderungNetzbetreiberzuordnungen": 1_000,
    "EinheitenBiomasse": 10_000,
    "EinheitenGasErzeuger": 100,
    "EinheitenGasSpeicher": 50,
    "EinheitenGasverbraucher": 100,
    "EinheitenGenehmigung": 10_000,
    "EinheitenGeothermieGrubengasDruckentspannung": 100,
    "EinheitenKernkraft": 2,
    "EinheitenSolar": 3_000_000,
    "EinheitenStromSpeicher": 500_000,
    "EinheitenStromVerbraucher": 1_000,
    "EinheitenVerbrennung": 10_000,
    "EinheitenWasser": 3_000,
    "EinheitenWind": 30_000,
    "Ertuechtigungen": 100,
    "GeloeschteUndDeaktivierteEinheiten": 10_000,
    "GeloeschteUndDeaktivierteMarktakteure": 10_000,
    "Lokationen": 3_000_000,
    "Marktakteure": 1_000_000,
    "MarktakteureUndRollen": 1_000,
    "Netzanschlusspunkte": 3_000_000,
    "Netze": 500,
}

# Columns that are filled from the MaStR catalogue (Katalogwerte)
# are not allowed to have integer values
CATALOG_COLUMNS = [
    "AnlageBetriebsstatus",
    "ArtDerSolaranlage",
    "ArtDerWasserkraftanlage",
    "Batterietechnologie",
    "Bundesland",
    "EinheitBetriebsstatus",
    "EinheitSystemstatus",
    "Einheittyp",
    "Einspeisungsart",
    "Energietraeger",
    "Ertuechtigungsart",
    "Hauptausrichtung",
    "Hauptbrennstoff",
    "Hersteller",
    "Land",
    "Lokationtyp",
    "Marktfunktion",
    "Marktgebiet",
    "Marktrolle",
    "NetzbetreiberpruefungStatus",
    "Nutzungsbereich",
    "Personenart",
    "Rechtsform",
    "Registergericht",
    "Spannungsebene",
    "Sparte",
    "Speicherart",
    "Technologie",
    "WeitereBrennstoffe",
    "WindAnLandOderAufSee",
]

# Columns that must not be empty (i.e. have at least one entry) in the tables that have them.
NOT_EMPTY_COLUMNS = [
    "Breitengrad",
    "Bruttoleistung",
    "EinheitBetriebsstatus",
    "Energietraeger",
    "Gemeindeschluessel",
    "Inbetriebnahmedatum",
    "Laengengrad",
    "Nettonennleistung",
    "Registrierungsdatum",
]

COLUMN_CHECKS = {
    "*MastrNummer": "{col} REGEXP '^[A-Z]{3}[0-9]{12}([A-Z]{2})?"
    "( *[,;] *[A-Z]{3}[0-9]{12})*$'",
    "*MastrNummern": "{col} REGEXP '^[A-Z]{3}[0-9]{12}( *[,;] *[A-Z]{3}[0-9]{12})*$'",
    "DatumDownload": "{col} REGEXP '^[0-9]{8}$'",
}

NOT_NULL_COLUMNS = ["DatenQuelle", "DatumDownload", "EinheitMastrNummer"]

STORAGE_TYPES = {
    "INTEGER": ["integer"],
    "BIGINT": ["integer"],
    "FLOAT": ["real", "integer"],
    "BOOLEAN": ["integer"],
    "VARCHAR": ["text"],
    "TEXT": ["text"],
    "DATE": ["text"],
    "DATETIME": ["text"],
}
# The storage type check looks at every single column of the database, so it
# only looks at the first rows of a table instead of doing full table scans.
STORAGE_TYPE_SAMPLE_SIZE = 10_000

failed_checks = 0


def report(description, failures):
    """Print the outcome of one check and remember whether it failed."""
    global failed_checks
    print(f"[{'FAIL' if failures else 'pass'}] {description}")
    for failure in failures:
        print(f"         {failure}")
    if failures:
        failed_checks += 1


def violations(connection, table, column, condition):
    """Describe the rows of a column that do not fulfil an SQL condition."""
    expression = condition.replace("{col}", f'"{column}"')
    where = f'"{column}" IS NOT NULL AND NOT ({expression})'
    rows = connection.execute(
        text(f'SELECT COUNT(*) FROM "{table}" WHERE {where}')
    ).scalar_one()
    if not rows:
        return None
    examples = connection.execute(
        text(f'SELECT DISTINCT "{column}" FROM "{table}" WHERE {where} LIMIT 5')
    )
    values = ", ".join(repr(row[0]) for row in examples)
    return f"{table}.{column}: {rows} rows, e.g. {values}"


def main():
    database = Mastr()

    # Comment this out to run the checks against a database downloaded before.
    database.download(method="bulk", date="today", bulk_cleansing=True)

    # Make the REGEXP operator available in SQLite. Connections opened before
    # now do not have it, so the engine is disposed afterwards.
    @event.listens_for(database.engine, "connect")
    def _on_connect(dbapi_connection, _connection_record):
        dbapi_connection.create_function(
            "regexp",
            2,
            lambda pattern, value: (
                value is not None and re.match(pattern, value) is not None
            ),
        )

    database.engine.dispose()
    print(f"\nChecking database {database.engine.url}\n")

    inspector = inspect(database.engine)
    tables = {
        table: [column["name"] for column in inspector.get_columns(table)]
        for table in inspector.get_table_names()
    }
    views = set(inspector.get_view_names())

    # Checks that need a column to fulfil a condition. Catalogue values must be
    # cleansed, so they never are only a digit.
    column_checks = {column: "{col} REGEXP '^[^0-9]'" for column in CATALOG_COLUMNS}
    column_checks.update(COLUMN_CHECKS)

    with database.engine.connect() as connection:
        check_all_expected_tables_exist(tables)
        check_no_unknown_tables_exist(tables)
        check_tables_have_minimum_row_counts(connection, tables)
        check_primary_keys_are_expected(inspector, tables)
        check_old_table_name_views_exist(connection, views)
        check_column_values_fulfil_conditions(connection, tables, column_checks)
        check_not_null_columns_are_filled(connection, tables)
        check_not_empty_columns_have_values(connection, tables)
        check_values_match_declared_column_types(connection, tables)
        check_boolean_columns_only_hold_0_and_1(connection, tables)

    print(f"\n{failed_checks} checks failed.")


def check_all_expected_tables_exist(tables):
    report(
        "All expected tables exist",
        [f"{table} is missing" for table in MINIMUM_ROW_COUNTS if table not in tables],
    )


def check_no_unknown_tables_exist(tables):
    report(
        "No unknown tables in the database",
        [
            f"{table} is unknown"
            for table in sorted(set(tables) - set(MINIMUM_ROW_COUNTS))
        ],
    )


def check_tables_have_minimum_row_counts(connection, tables):
    failures = []
    for table, minimum in sorted(MINIMUM_ROW_COUNTS.items()):
        if table not in tables:
            continue
        rows = connection.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one()
        if rows < minimum:
            failures.append(f"{table}: {rows} rows, expected at least {minimum}")
    report("Tables have at least the expected number of rows", failures)


def check_primary_keys_are_expected(inspector, tables):
    # A table with a primary key that open-mastr does not expect means that
    # the MaStR changed a table, or added one that open-mastr does not know.
    failures = []
    for table in sorted(tables):
        if table not in MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS:
            failures.append(f"{table}: open-mastr does not know this table")
            continue
        # Tables without a natural primary key get an artificial one.
        expected = MASTR_TABLE_NAME_TO_PRIMARY_KEY_COLUMNS[table] or {"OpenMastrId"}
        found = set(inspector.get_pk_constraint(table)["constrained_columns"])
        if found != expected:
            failures.append(
                f"{table}: primary key is {sorted(found)}, expected {sorted(expected)}"
            )
    report("Primary keys are the ones open-mastr expects", failures)


def check_old_table_name_views_exist(connection, views):
    # The views with the pre-0.16 table names exist and can be read.
    failures = []
    for table, view in sorted(OLD_MASTR_TABLE_TO_OLD_DB_TABLE_NAME.items()):
        # Catalogue tables are dropped after cleansing, so they have no view.
        if table not in MINIMUM_ROW_COUNTS:
            continue
        if view not in views:
            failures.append(f"view {view} is missing")
        else:
            connection.execute(text(f'SELECT * FROM "{view}" LIMIT 1')).fetchall()
    report("Views with the old table names exist", failures)


def check_column_values_fulfil_conditions(connection, tables, column_checks):
    for pattern, condition in sorted(column_checks.items()):
        failures = []
        for table, columns in sorted(tables.items()):
            for column in columns:
                matches = (
                    column.endswith(pattern[1:])
                    if pattern.startswith("*")
                    else column == pattern
                )
                if matches:
                    failures.append(violations(connection, table, column, condition))
        report(f"{pattern}: {condition}", [f for f in failures if f])


def check_not_null_columns_are_filled(connection, tables):
    for column in NOT_NULL_COLUMNS:
        failures = []
        for table, columns in sorted(tables.items()):
            if column in columns:
                rows = connection.execute(
                    text(f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" IS NULL')
                ).scalar_one()
                if rows:
                    failures.append(f"{table}.{column}: {rows} rows are NULL")
        report(f"{column} is filled", failures)


def check_not_empty_columns_have_values(connection, tables):
    for column in NOT_EMPTY_COLUMNS:
        failures = []
        for table, columns in sorted(tables.items()):
            if column in columns:
                rows = connection.execute(
                    text(f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" IS NOT NULL')
                ).scalar_one()
                if not rows:
                    failures.append(f"{table}.{column}: all rows are NULL")
        report(f"{column} is not empty", failures)


def check_values_match_declared_column_types(connection, tables):
    wrong_types = []
    for table in sorted(tables):
        for column in connection.execute(text(f'PRAGMA table_info("{table}")')):
            name, declared = column[1], column[2].upper()
            if declared not in STORAGE_TYPES:
                wrong_types.append(f"{table}.{name}: unknown type {declared}")
                continue
            sample = (
                f'(SELECT "{name}" AS value FROM "{table}"'
                f" LIMIT {STORAGE_TYPE_SAMPLE_SIZE})"
            )
            stored = {
                row[0]
                for row in connection.execute(
                    text(f"SELECT DISTINCT typeof(value) FROM {sample}")
                )
            } - {"null"}
            if not stored <= set(STORAGE_TYPES[declared]):
                wrong_types.append(
                    f"{table}.{name} is declared as {declared}"
                    f" but stored as {sorted(stored)}"
                )
    report("Values match their declared column type", wrong_types)


def check_boolean_columns_only_hold_0_and_1(connection, tables):
    wrong_booleans = []
    for table in sorted(tables):
        for column in connection.execute(text(f'PRAGMA table_info("{table}")')):
            name, declared = column[1], column[2].upper()
            if declared != "BOOLEAN":
                continue
            sample = (
                f'(SELECT "{name}" AS value FROM "{table}"'
                f" LIMIT {STORAGE_TYPE_SAMPLE_SIZE})"
            )
            values = {
                row[0]
                for row in connection.execute(
                    text(f"SELECT DISTINCT value FROM {sample}")
                )
            } - {None}
            if not values <= {0, 1}:
                wrong_booleans.append(f"{table}.{name} holds {sorted(values)}")
    report("Boolean columns only hold 0 and 1", wrong_booleans)


if __name__ == "__main__":
    main()
