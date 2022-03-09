import open_mastr.orm as orm


def test_technology_to_include_tables():
    include_tables_list = [
        "anlageneegwind",
        "einheitenwind",
        "anlageneegwasser",
        "einheitenwasser",
    ]
    include_tables_str = ["einheitenstromverbraucher", "einheitenverbrennung"]

    assert include_tables_list == orm.technology_to_include_tables(["wind", "hydro"])
    assert include_tables_str == orm.technology_to_include_tables(
        "electricity_consumer"
    )
