# system catalog is the mapping for the entries within the columns
# Marktfunktion, Lokationtyp and Einheittyp
# The values for the system catalog can be found in the pdf of the bulk download
# documentation: https://www.marktstammdatenregister.de/MaStR/Datendownload

system_catalog = {
    "Marktfunktion": {
        1: "Stromnetzbetreiber",
        2: "Anlagenbetreiber",
        3: "Akteur im Strommarkt",
        4: "Organisierter Marktplatz",
        5: "Behörde, Verband, Institution",
        6: "Sonstiger Marktakteur",
        7: "Bundesnetzagentur",
        8: "Gasnetzbetreiber",
        9: "Akteur im Gasmarkt",
        10: "Supportpartner",
    },
    "Lokationtyp": {
        1: "Stromerzeugungslokation",
        2: "Stromverbrauchslokation",
        3: "Gaserzeugungslokation",
        4: "Gasverbrauchslokation",
    },
    "Einheittyp": {
        1: "Solareinheit",
        2: "Windeinheit",
        3: "Biomasse",
        4: "Wasser",
        5: "Geothermie",
        6: "Verbrennung",
        7: "Kernenergie",
        8: "Stromspeichereinheit",
        9: "Stromverbrauchseinheit",
        10: "Gasverbrauchseinheit",
        11: "Gaserzeugungseinheit",
        12: "Gasspeichereinheit",
    },
}
