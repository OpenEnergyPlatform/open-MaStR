import logging
from collections.abc import Mapping

from sqlalchemy import Engine, MetaData, Table, inspect, select, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.ddl import DDLElement

log = logging.getLogger("open-MaStR")


class CreateView(DDLElement):
    def __init__(self, name: str, selectable):
        self.name = name
        self.selectable = selectable


@compiles(CreateView)
def _compile_create_view(element, compiler, **kw):
    selectable_sql = compiler.sql_compiler.process(
        element.selectable, literal_binds=True
    )
    return f"CREATE VIEW {element.name} AS {selectable_sql}"


OLD_MASTR_TABLE_TO_OLD_DB_TABLE_NAME = {
    "AnlagenEegBiomasse": "biomass_eeg",
    "AnlagenEegGeothermieGrubengasDruckentspannung": "gsgk_eeg",
    "AnlagenEegSolar": "solar_eeg",
    "AnlagenEegSpeicher": "storage_eeg",
    "AnlagenEegWasser": "hydro_eeg",
    "AnlagenEegWind": "wind_eeg",
    "AnlagenGasSpeicher": "gas_storage",
    "AnlagenKwk": "kwk",
    "AnlagenStromSpeicher": "storage_units",
    "Bilanzierungsgebiete": "balancing_area",
    "EinheitenAenderungNetzbetreiberzuordnungen": "changed_dso_assignment",
    "EinheitenBiomasse": "biomass_extended",
    "EinheitenGasErzeuger": "gas_producer",
    "EinheitenGasSpeicher": "gas_storage_extended",
    "EinheitenGasverbraucher": "gas_consumer",
    "EinheitenGenehmigung": "permit",
    "EinheitenGeothermieGrubengasDruckentspannung": "gsgk_extended",
    "EinheitenKernkraft": "nuclear_extended",
    "EinheitenSolar": "solar_extended",
    "EinheitenStromSpeicher": "storage_extended",
    "EinheitenStromVerbraucher": "electricity_consumer",
    "EinheitenVerbrennung": "combustion_extended",
    "EinheitenWasser": "hydro_extended",
    "EinheitenWind": "wind_extended",
    "Ertuechtigungen": "retrofit_units",
    "GeloeschteUndDeaktivierteEinheiten": "deleted_units",
    "GeloeschteUndDeaktivierteMarktakteure": "deleted_market_actors",
    "Katalogkategorien": "catalog_categories",
    "Katalogwerte": "catalog_values",
    "Lokationen": "locations",
    "Marktakteure": "market_actors",
    "MarktakteureUndRollen": "market_actors_and_roles",
    "Netzanschlusspunkte": "grid_connections",
    "Netze": "grids",
}


def _create_view(
    engine: Engine,
    db_table: Table,
    view_name: str,
) -> None:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    views = inspector.get_view_names()

    with engine.begin() as conn:
        if view_name in views:
            conn.execute(text(f'DROP VIEW "{view_name}"'))
        elif view_name in tables:
            conn.execute(text(f'DROP TABLE "{view_name}"'))
        conn.execute(CreateView(view_name, select(db_table)))


def create_views(engine: Engine, mastr_table_to_db_table: Mapping[str, Table]) -> None:
    for mastr_table, db_table in mastr_table_to_db_table.items():
        old_table_name = OLD_MASTR_TABLE_TO_OLD_DB_TABLE_NAME.get(mastr_table)
        if not old_table_name:
            log.debug(
                f"No old table name known for MaStR table {mastr_table}."
                " Not creating any view."
            )
            continue
        if old_table_name == db_table.name:
            log.debug(
                f"Old table name {old_table_name} was the same as the current name."
                " Not creating any view."
            )
            continue

        log.info(f"Creating view {old_table_name} mirroring table {db_table.name}")
        _create_view(engine=engine, db_table=db_table, view_name=old_table_name)

