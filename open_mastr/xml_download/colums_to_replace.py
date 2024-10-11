# system catalog is the mapping for the entries within the two columns
# Marktfunktionen und Lokationstyp (entry 1 is mapped to Stromnetzbetreiber
# in the column Marktfunktionen)
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
}

# columns to replace lists all columns where the entries have
# to be replaced according to the tables katalogwerte and katalogeinträge
# from the bulk download of the MaStR

columns_replace_list = [
    # anlageneegsolar
    "AnlageBetriebsstatus",
    # anlageneegspeicher
    # anlagenstromspeicher
    # einheitensolar
    "Land",
    "Bundesland",
    "EinheitSystemstatus",
    "EinheitBetriebsstatus",
    "Energietraeger",
    "Einspeisungsart",
    "GemeinsamerWechselrichterMitSpeicher",
    "Lage",
    "Leistungsbegrenzung",
    "Hauptausrichtung",
    "HauptausrichtungNeigungswinkel",
    "Nutzungsbereich",
    "Nebenausrichtung",
    "NebenausrichtungNeigungswinkel",
    "ArtDerFlaecheIds",
    # einheitenstromspeicher
    "AcDcKoppelung",
    "Batterietechnologie",
    "Technologie",
    "Pumpspeichertechnologie",
    "Einsatzort",
    # geloeschteunddeaktivierteEinheiten
    # geloeschteunddeaktivierteMarktAkteure
    "MarktakteurStatus",
    # lokationen
    # marktakteure
    "Personenart",
    "Rechtsform",
    "HauptwirtdschaftszweigAbteilung",
    "HauptwirtdschaftszweigGruppe",
    "HauptwirtdschaftszweigAbschnitt",
    "Registergericht",
    "LandAnZustelladresse",
    # netzanschlusspunkte
    "Gasqualitaet",
    "Spannungsebene",
    # anlageneegbiomasse
    # anlageneeggeosolarthermiegrubenklaerschlammdruckentspannung
    # anlageneegwasser
    # anlageneegwind
    # anlagengasspeicher
    # anlagenkwk
    # bilanzierungsgebiete
    # einheitenaenderungnetzbetreiberzuordnungen
    "ArtDerAenderung",
    # einheitenbiomasse
    "Hauptbrennstoff",
    "Biomasseart",
    # einheitengaserzeuger
    # einheitengasspeicher
    "Speicherart",
    # einheitengasverbraucher
    # einheitengenehmigung
    "Art",
    # einheitengeosolarthermiegrubenklaerschlammdruckentspannung
    # einheitenkernkraft
    # einheitenstromverbraucher
    "ArtAbschaltbareLast",
    # einheitentypen
    # einheitenverbrennung
    "WeitererHauptbrennstoff",
    "WeitereBrennstoffe",
    "ArtDerStilllegung",
    # einheitenwasser
    "ArtDesZuflusses",
    "ArtDerWasserkraftanlage",
    # marktrollen
    # netze
    "Sparte",
    # einheitenwind
    "Lage",
    "Hersteller",
    "Seelage",
    "ClusterNordsee",
    "ClusterOstsee",
    # various tables
    "NetzbetreiberpruefungStatus",
]
