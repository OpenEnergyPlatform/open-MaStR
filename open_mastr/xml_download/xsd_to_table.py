

class SqlalchemyMastrModelMaker:
    MASTR_COLUMN_TYPE_TO_SQLALCHEMY_TYPE = {
        MastrColumnType.STRING: String,
        MastrColumnType.INTEGER: Integer,
        MastrColumnType.FLOAT: Float,
        MastrColumnType.DATE: Date,
        MastrColumnType.DATETIME: DateTime(timezone=True),
        MastrColumnType.BOOLEAN: Boolean,
        MastrColumnType.CATALOG_VALUE: Integer,
    }

    @classmethod
    def make_sqlalchemy_mastr_model(
        cls,
        table: MastrTableDescription,
        primary_key_columns: set[str],
        base: DeclarativeBase,
        mixins: tuple[type, ...] = tuple(),
    ):
        namespace = {
            "__tablename__": table.table_name,
            "__annotations__": {},
        }

        for col in table.columns:
            sa_type = cls.MASTR_COLUMN_TYPE_TO_SQLALCHEMY_TYPE[col.type]
            kwargs = {"primary_key": True} if col.name in primary_key_columns else {"nullable": True}
            namespace[col.name] = mapped_column(sa_type, **kwargs)

        bases = (base,) + mixins
        return type(table.instance_name, bases, namespace)


class Base(DeclarativeBase):
    pass


class ParentAllTables(object):
    DatenQuelle = Column(String)
    DatumDownload = Column(Date)


def generate_sqlalchemy_models(xsd_file: str) -> str:
    schema = xmlschema.XMLSchema(xsd_file)
    table = MastrTableDescription.from_xml_schema(schema)

    model = SqlalchemyMastrModelMaker.make_sqlalchemy_mastr_model(
        table=table,
        primary_key_columns={"EinheitMastrNummer"},
        base=Base,
        mixins=(ParentAllTables,)
    )


if __name__ == "__main__":
    import sys
    xsd_path = sys.argv[1]
    generate_sqlalchemy_models(xsd_path)

