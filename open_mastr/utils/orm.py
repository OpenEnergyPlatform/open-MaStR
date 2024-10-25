from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Sequence,
    DateTime,
    Boolean,
    func,
    Date,
    JSON,
)


class Base(DeclarativeBase):
    pass


class ParentAllTables(object):
    DatenQuelle = Column(String)
    DatumDownload = Column(Date)


class BasicUnit(Base):
    __tablename__ = "basic_units"

    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Name = Column(String)
    Einheitart = Column(String)
    Einheittyp = Column(String)
    Standort = Column(String)
    Bruttoleistung = Column(Float)
    Erzeugungsleistung = Column(Float)
    EinheitBetriebsstatus = Column(String)
    Anlagenbetreiber = Column(String)
    EegMastrNummer = Column(String)
    KwkMastrNummer = Column(String)
    SpeMastrNummer = Column(String)
    GenMastrNummer = Column(String)
    BestandsanlageMastrNummer = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(String)
    EinheitSystemstatus = Column(String)


class AdditionalDataRequested(Base):
    __tablename__ = "additional_data_requested"

    id = Column(
        Integer,
        Sequence("additional_data_requested_id_seq"),
        primary_key=True,
    )
    EinheitMastrNummer = Column(String)
    additional_data_id = Column(String)
    technology = Column(String)
    data_type = Column(String)
    request_date = Column(DateTime(timezone=True), default=func.now())


class MissedAdditionalData(Base):
    __tablename__ = "missed_additional_data"

    id = Column(
        Integer,
        Sequence("additional_data_missed_id_seq"),
        primary_key=True,
    )
    additional_data_id = Column(String)
    reason = Column(String)
    download_date = Column(DateTime(timezone=True), default=func.now())


class Extended(object):
    NetzbetreiberMastrNummer = Column(String)
    Registrierungsdatum = Column(Date)
    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    NetzbetreiberpruefungStatus = Column(String)
    NetzbetreiberpruefungDatum = Column(Date)
    AnlagenbetreiberMastrNummer = Column(String)
    Land = Column(String)
    Bundesland = Column(String)
    Landkreis = Column(String)
    Gemeinde = Column(String)
    Gemeindeschluessel = Column(String)
    Postleitzahl = Column(String)
    Gemarkung = Column(String)
    FlurFlurstuecknummern = Column(String)
    Strasse = Column(String)
    StrasseNichtGefunden = Column(Boolean)
    Hausnummer = Column(String)
    HausnummerNichtGefunden = Column(Boolean)
    Adresszusatz = Column(String)
    Ort = Column(String)
    Laengengrad = Column(Float)
    Breitengrad = Column(Float)
    UtmZonenwert = Column(String)
    UtmEast = Column(Float)
    UtmNorth = Column(Float)
    GaussKruegerHoch = Column(Float)
    GaussKruegerRechts = Column(Float)
    Meldedatum = Column(Date)
    GeplantesInbetriebnahmedatum = Column(Date)
    Inbetriebnahmedatum = Column(Date)
    DatumEndgueltigeStilllegung = Column(Date)
    DatumBeginnVoruebergehendeStilllegung = Column(Date)
    DatumBeendigungVorlaeufigenStilllegung = Column(Date)
    DatumWiederaufnahmeBetrieb = Column(Date)
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)
    BestandsanlageMastrNummer = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(Boolean)
    AltAnlagenbetreiberMastrNummer = Column(String)
    DatumDesBetreiberwechsels = Column(Date)
    DatumRegistrierungDesBetreiberwechsels = Column(Date)
    NameStromerzeugungseinheit = Column(String)
    Weic = Column(String)
    WeicDisplayName = Column(String)
    Kraftwerksnummer = Column(String)
    Energietraeger = Column(String)
    Bruttoleistung = Column(Float)
    Nettonennleistung = Column(Float)
    AnschlussAnHoechstOderHochSpannung = Column(Boolean)
    Schwarzstartfaehigkeit = Column(Boolean)
    Inselbetriebsfaehigkeit = Column(Boolean)
    Einsatzverantwortlicher = Column(String)
    FernsteuerbarkeitNb = Column(Boolean)
    FernsteuerbarkeitDv = Column(Boolean)
    FernsteuerbarkeitDr = Column(Boolean)
    Einspeisungsart = Column(String)
    PraequalifiziertFuerRegelenergie = Column(Boolean)
    GenMastrNummer = Column(String)
    Netzbetreiberzuordnungen = Column(String)
    ReserveartNachDemEnWG = Column(String)
    DatumUeberfuehrungInReserve = Column(Date)
    # from bulk download
    Hausnummer_nv = Column(Boolean)
    Weic_nv = Column(Boolean)
    Kraftwerksnummer_nv = Column(Boolean)


class WindExtended(Extended, ParentAllTables, Base):
    __tablename__ = "wind_extended"

    # wind specific attributes
    NameWindpark = Column(String)
    Lage = Column(String)
    Seelage = Column(String)
    ClusterOstsee = Column(String)
    ClusterNordsee = Column(String)
    Hersteller = Column(String)
    HerstellerId = Column(String)
    Technologie = Column(String)
    Typenbezeichnung = Column(String)
    Nabenhoehe = Column(Float)
    Rotordurchmesser = Column(Float)
    Rotorblattenteisungssystem = Column(Boolean)
    AuflageAbschaltungLeistungsbegrenzung = Column(Boolean)
    AuflagenAbschaltungSchallimmissionsschutzNachts = Column(Boolean)
    AuflagenAbschaltungSchallimmissionsschutzTagsueber = Column(Boolean)
    AuflagenAbschaltungSchattenwurf = Column(Boolean)
    AuflagenAbschaltungTierschutz = Column(Boolean)
    AuflagenAbschaltungEiswurf = Column(Boolean)
    AuflagenAbschaltungSonstige = Column(Boolean)
    Wassertiefe = Column(Float)
    Kuestenentfernung = Column(Float)
    Buergerenergie = Column(Boolean)
    Nachtkennzeichen = Column(Boolean)
    EegMastrNummer = Column(String)


class SolarExtended(Extended, ParentAllTables, Base):
    __tablename__ = "solar_extended"

    ZugeordneteWirkleistungWechselrichter = Column(Float)
    GemeinsamerWechselrichterMitSpeicher = Column(String)
    AnzahlModule = Column(Integer)
    Lage = Column(String)
    Leistungsbegrenzung = Column(String)
    EinheitlicheAusrichtungUndNeigungswinkel = Column(Boolean)
    Hauptausrichtung = Column(String)
    HauptausrichtungNeigungswinkel = Column(String)
    Nebenausrichtung = Column(String)
    NebenausrichtungNeigungswinkel = Column(String)
    InAnspruchGenommeneFlaeche = Column(Float)
    ArtDerFlaeche = Column(String)
    InAnspruchGenommeneLandwirtschaftlichGenutzteFlaeche = Column(Float)
    Nutzungsbereich = Column(String)
    Buergerenergie = Column(Boolean)
    EegMastrNummer = Column(String)
    ArtDerFlaecheIds = Column(String)
    Zaehlernummer = Column(String)


class BiomassExtended(Extended, ParentAllTables, Base):
    __tablename__ = "biomass_extended"

    Hauptbrennstoff = Column(String)
    Biomasseart = Column(String)
    Technologie = Column(String)
    EegMastrNummer = Column(String)
    KwkMastrNummer = Column(String)


class CombustionExtended(Extended, ParentAllTables, Base):
    __tablename__ = "combustion_extended"

    NameKraftwerk = Column(String)
    NameKraftwerksblock = Column(String)
    DatumBaubeginn = Column(Date)
    SteigerungNettonennleistungKombibetrieb = Column(Float)
    AnlageIstImKombibetrieb = Column(Boolean)
    MastrNummernKombibetrieb = Column(String)
    NetzreserveAbDatum = Column(Date)
    SicherheitsbereitschaftAbDatum = Column(Date)
    Hauptbrennstoff = Column(String)
    WeitererHauptbrennstoff = Column(String)
    WeitereBrennstoffe = Column(String)
    VerknuepfteErzeugungseinheiten = Column(String)
    BestandteilGrenzkraftwerk = Column(Boolean)
    NettonennleistungDeutschland = Column(Float)
    AnteiligNutzungsberechtigte = Column(String)
    Notstromaggregat = Column(Boolean)
    Einsatzort = Column(String)
    KwkMastrNummer = Column(String)
    Technologie = Column(String)
    AusschliesslicheVerwendungImKombibetrieb = Column(Boolean)


class GsgkExtended(Extended, ParentAllTables, Base):
    __tablename__ = "gsgk_extended"

    Technologie = Column(String)
    KwkMastrNummer = Column(String)
    EegMastrNummer = Column(String)


class HydroExtended(Extended, ParentAllTables, Base):
    __tablename__ = "hydro_extended"

    NameKraftwerk = Column(String)
    ArtDerWasserkraftanlage = Column(String)
    MinderungStromerzeugung = Column(Boolean)
    BestandteilGrenzkraftwerk = Column(Boolean)
    NettonennleistungDeutschland = Column(Float)
    ArtDesZuflusses = Column(String)
    EegMastrNummer = Column(String)


class NuclearExtended(Extended, ParentAllTables, Base):
    __tablename__ = "nuclear_extended"

    NameKraftwerk = Column(String)
    NameKraftwerksblock = Column(String)
    Technologie = Column(String)


class StorageExtended(Extended, ParentAllTables, Base):
    __tablename__ = "storage_extended"

    Einsatzort = Column(String)
    AcDcKoppelung = Column(String)
    Batterietechnologie = Column(String)
    PumpbetriebLeistungsaufnahme = Column(Float)
    PumpbetriebKontinuierlichRegelbar = Column(Boolean)
    Pumpspeichertechnologie = Column(String)
    Notstromaggregat = Column(Boolean)
    BestandteilGrenzkraftwerk = Column(Boolean)
    NettonennleistungDeutschland = Column(Float)
    ZugeordneteWirkleistungWechselrichter = Column(Float)
    NutzbareSpeicherkapazitaet = Column(Float)
    SpeMastrNummer = Column(String)
    EegMastrNummer = Column(String)
    EegAnlagentyp = Column(String)
    Technologie = Column(String)
    LeistungsaufnahmeBeimEinspeichern = Column(Float)


class Eeg(object):
    Registrierungsdatum = Column(Date)
    EegMastrNummer = Column(String, primary_key=True)
    Meldedatum = Column(Date)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    EegInbetriebnahmedatum = Column(Date)
    VerknuepfteEinheit = Column(String)
    AnlagenschluesselEeg = Column(String)
    AusschreibungZuschlag = Column(Boolean)
    AnlagenkennzifferAnlagenregister = Column(String)
    AnlagenkennzifferAnlagenregister_nv = Column(Boolean)
    Netzbetreiberzuordnungen = Column(String)


class WindEeg(Eeg, ParentAllTables, Base):
    __tablename__ = "wind_eeg"

    PrototypAnlage = Column(Boolean)
    PilotAnlage = Column(Boolean)
    InstallierteLeistung = Column(Float)
    VerhaeltnisErtragsschaetzungReferenzertrag = Column(Float)
    VerhaeltnisReferenzertragErtrag5Jahre = Column(Float)
    VerhaeltnisReferenzertragErtrag10Jahre = Column(Float)
    VerhaeltnisReferenzertragErtrag15Jahre = Column(Float)
    Zuschlagsnummer = Column(String)
    AnlageBetriebsstatus = Column(String)
    VerhaeltnisErtragsschaetzungReferenzertrag_nv = Column(Boolean)
    VerhaeltnisReferenzertragErtrag5Jahre_nv = Column(Boolean)
    VerhaeltnisReferenzertragErtrag10Jahre_nv = Column(Boolean)
    VerhaeltnisReferenzertragErtrag15Jahre_nv = Column(Boolean)


class SolarEeg(Eeg, ParentAllTables, Base):
    __tablename__ = "solar_eeg"

    InanspruchnahmeZahlungNachEeg = Column(Boolean)
    InstallierteLeistung = Column(Float)
    RegistrierungsnummerPvMeldeportal = Column(String)
    MieterstromRegistrierungsdatum = Column(Date)
    MieterstromZugeordnet = Column(Boolean)
    MieterstromMeldedatum = Column(Date)
    MieterstromErsteZuordnungZuschlag = Column(Date)
    ZugeordneteGebotsmenge = Column(Float)
    Zuschlagsnummer = Column(String)
    AnlageBetriebsstatus = Column(String)
    RegistrierungsnummerPvMeldeportal_nv = Column(Boolean)


class BiomassEeg(Eeg, ParentAllTables, Base):
    __tablename__ = "biomass_eeg"

    InstallierteLeistung = Column(Float)
    AusschliesslicheVerwendungBiomasse = Column(Boolean)
    Zuschlagsnummer = Column(String)
    BiogasInanspruchnahmeFlexiPraemie = Column(Boolean)
    BiogasDatumInanspruchnahmeFlexiPraemie = Column(Date)
    BiogasLeistungserhoehung = Column(Boolean)
    BiogasDatumLeistungserhoehung = Column(Date)
    BiogasUmfangLeistungserhoehung = Column(Float)
    BiogasGaserzeugungskapazitaet = Column(Float)
    Hoechstbemessungsleistung = Column(Float)
    BiomethanErstmaligerEinsatz = Column(Date)
    AnlageBetriebsstatus = Column(String)
    BiogasGaserzeugungskapazitaet_nv = Column(Boolean)
    BiomethanErstmaligerEinsatz_nv = Column(Boolean)


class GsgkEeg(Eeg, ParentAllTables, Base):
    __tablename__ = "gsgk_eeg"

    InstallierteLeistung = Column(Float)
    AnlageBetriebsstatus = Column(String)


class HydroEeg(Eeg, ParentAllTables, Base):
    __tablename__ = "hydro_eeg"

    InstallierteLeistung = Column(Float)
    AnlageBetriebsstatus = Column(String)
    Ertuechtigung = Column(JSON)
    ErtuechtigungIds = Column(String)


class StorageEeg(Eeg, ParentAllTables, Base):
    __tablename__ = "storage_eeg"

    eegAnlagenschluessel = Column(String)
    eegZuschlagsnummer = Column(String)
    eegAusschreibungZuschlag = Column(Boolean)


class Kwk(ParentAllTables, Base):
    __tablename__ = "kwk"

    Registrierungsdatum = Column(Date)
    KwkMastrNummer = Column(String, primary_key=True)
    Zuschlagnummer = Column(String)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Inbetriebnahmedatum = Column(Date)
    Meldedatum = Column(Date)
    ThermischeNutzleistung = Column(Float)
    ElektrischeKwkLeistung = Column(Float)
    VerknuepfteEinheiten = Column(String)
    AnlageBetriebsstatus = Column(String)
    AusschreibungZuschlag = Column(Boolean)
    Netzbetreiberzuordnungen = Column(String)


class Permit(ParentAllTables, Base):
    __tablename__ = "permit"

    Registrierungsdatum = Column(Date)
    GenMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Art = Column(String)
    Datum = Column(Date)
    Behoerde = Column(String)
    Aktenzeichen = Column(String)
    Frist = Column(Date)
    WasserrechtsNummer = Column(String)
    WasserrechtAblaufdatum = Column(Date)
    Meldedatum = Column(Date)
    VerknuepfteEinheiten = Column(String)
    Frist_nv = Column(Boolean)
    WasserrechtAblaufdatum_nv = Column(Boolean)
    Netzbetreiberzuordnungen = Column(String)


class LocationBasic(Base):
    __tablename__ = "locations_basic"

    LokationMastrNummer = Column(String, primary_key=True)
    NameDerTechnischenLokation = Column(String)
    Lokationtyp = Column(String)
    AnzahlNetzanschlusspunkte = Column(Integer)


class LocationExtended(ParentAllTables, Base):
    __tablename__ = "locations_extended"

    MastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    NameDerTechnischenLokation = Column(String)
    VerknuepfteEinheiten = Column(String)
    Netzanschlusspunkte = Column(String)
    Lokationtyp = Column(String)


class AdditionalLocationsRequested(Base):
    __tablename__ = "additional_locations_requested"

    id = Column(
        Integer,
        Sequence("additional_locations_requested_id_seq"),
        primary_key=True,
    )
    LokationMastrNummer = Column(String)
    location_type = Column(String)
    request_date = Column(DateTime(timezone=True), default=func.now())


class MissedExtendedLocation(ParentAllTables, Base):
    __tablename__ = "missed_extended_location_data"

    id = Column(
        Integer,
        Sequence("additional_location_data_missed_id_seq"),
        primary_key=True,
    )
    LokationMastrNummer = Column(String)
    reason = Column(String)
    download_date = Column(DateTime(timezone=True), default=func.now())


class GasStorage(ParentAllTables, Base):
    __tablename__ = "gas_storage"

    MastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Speichername = Column(String)
    Registrierungsdatum = Column(Date)
    AnlageBetriebsstatus = Column(String)
    VerknuepfteEinheit = Column(String)


class GasStorageExtended(ParentAllTables, Base):
    __tablename__ = "gas_storage_extended"
    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    NetzbetreiberpruefungStatus = Column(String)
    NetzbetreiberpruefungDatum = Column(Date)
    AnlagenbetreiberMastrNummer = Column(String)
    Land = Column(String)
    Bundesland = Column(String)
    Landkreis = Column(String)
    Gemeinde = Column(String)
    Gemeindeschluessel = Column(String)
    Postleitzahl = Column(String)
    Ort = Column(String)
    Strasse = Column(String)
    StrasseNichtGefunden = Column(Integer)
    Hausnummer = Column(String)
    Hausnummer_nv = Column(Integer)
    HausnummerNichtGefunden = Column(Integer)
    Laengengrad = Column(Float)
    Breitengrad = Column(Float)
    Registrierungsdatum = Column(String)
    Inbetriebnahmedatum = Column(String)
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(Integer)
    NameGasspeicher = Column(String)
    Speicherart = Column(String)
    MaximalNutzbaresArbeitsgasvolumen = Column(Float)
    MaximaleEinspeicherleistung = Column(Float)
    MaximaleAusspeicherleistung = Column(Float)
    DurchschnittlicherBrennwert = Column(Float)
    Weic = Column(String)
    Weic_Na = Column(Integer)
    SpeicherMastrNummer = Column(String)
    Gemarkung = Column(String)
    FlurFlurstuecknummern = Column(String)
    Adresszusatz = Column(String)
    DatumBeginnVoruebergehendeStilllegung = Column(Date)
    DatumDesBetreiberwechsels = Column(Date)
    DatumRegistrierungDesBetreiberwechsels = Column(Date)
    DatumEndgueltigeStilllegung = Column(Date)


class StorageUnits(ParentAllTables, Base):
    __tablename__ = "storage_units"
    MastrNummer = Column(String, primary_key=True)
    Registrierungsdatum = Column(Date)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    NutzbareSpeicherkapazitaet = Column(Float)
    VerknuepfteEinheit = Column(String)
    AnlageBetriebsstatus = Column(String)


class BalancingArea(ParentAllTables, Base):
    __tablename__ = "balancing_area"

    Id = Column(Integer, primary_key=True)
    Yeic = Column(String)
    RegelzoneNetzanschlusspunkt = Column(String)
    BilanzierungsgebietNetzanschlusspunkt = Column(String)


class GasProducer(ParentAllTables, Base):
    __tablename__ = "gas_producer"

    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    NetzbetreiberpruefungStatus = Column(String)
    NetzbetreiberpruefungDatum = Column(Date)
    AnlagenbetreiberMastrNummer = Column(String)
    Land = Column(String)
    Bundesland = Column(String)
    Landkreis = Column(String)
    Gemeinde = Column(String)
    Gemeindeschluessel = Column(String)
    Postleitzahl = Column(String)
    Ort = Column(String)
    Registrierungsdatum = Column(Date)
    Inbetriebnahmedatum = Column(Date)
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(Integer)
    NameGaserzeugungseinheit = Column(String)
    SpeicherMastrNummer = Column(String)
    Strasse = Column(String)
    StrasseNichtGefunden = Column(Integer)
    Hausnummer = Column(String)
    Hausnummer_nv = Column(Integer)
    HausnummerNichtGefunden = Column(Integer)
    Adresszusatz = Column(String)
    Laengengrad = Column(Float)
    Breitengrad = Column(Float)
    Technologie = Column(String)
    Erzeugungsleistung = Column(Float)
    DatumDesBetreiberwechsels = Column(Date)
    DatumRegistrierungDesBetreiberwechsels = Column(Date)
    Gemarkung = Column(String)
    FlurFlurstuecknummern = Column(String)
    GeplantesInbetriebnahmedatum = Column(Date)
    DatumBeginnVoruebergehendeStilllegung = Column(Date)
    DatumEndgueltigeStilllegung = Column(Date)


class GasConsumer(ParentAllTables, Base):
    __tablename__ = "gas_consumer"

    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    NetzbetreiberpruefungStatus = Column(String)
    NetzbetreiberpruefungDatum = Column(Date)
    AnlagenbetreiberMastrNummer = Column(String)
    Land = Column(String)
    Bundesland = Column(String)
    Landkreis = Column(String)
    Gemeinde = Column(String)
    Gemeindeschluessel = Column(String)
    Postleitzahl = Column(String)
    Ort = Column(String)
    Strasse = Column(String)
    StrasseNichtGefunden = Column(Integer)
    Hausnummer = Column(String)
    Hausnummer_nv = Column(Integer)
    HausnummerNichtGefunden = Column(Integer)
    Laengengrad = Column(Float)
    Breitengrad = Column(Float)
    Registrierungsdatum = Column(String)
    Inbetriebnahmedatum = Column(String)
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(Integer)
    NameGasverbrauchsseinheit = Column(String)
    EinheitDientDerStromerzeugung = Column(String)
    MaximaleGasbezugsleistung = Column(Float)
    VerknuepfteEinheit = Column(String)
    GeplantesInbetriebnahmedatum = Column(Date)
    Adresszusatz = Column(String)
    Gemarkung = Column(String)
    FlurFlurstuecknummern = Column(String)
    DatumDesBetreiberwechsels = Column(Date)
    DatumRegistrierungDesBetreiberwechsels = Column(Date)
    DatumEndgueltigeStilllegung = Column(Date)
    DatumBeginnVoruebergehendeStilllegung = Column(Date)


class ElectricityConsumer(ParentAllTables, Base):
    __tablename__ = "electricity_consumer"

    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    NetzbetreiberpruefungStatus = Column(String)
    NetzbetreiberpruefungDatum = Column(Date)
    AnlagenbetreiberMastrNummer = Column(String)
    Land = Column(String)
    Bundesland = Column(String)
    Landkreis = Column(String)
    Gemeinde = Column(String)
    Gemeindeschluessel = Column(String)
    Postleitzahl = Column(String)
    Ort = Column(String)
    Strasse = Column(String)
    StrasseNichtGefunden = Column(Integer)
    Hausnummer = Column(String)
    Hausnummer_nv = Column(Integer)
    HausnummerNichtGefunden = Column(Integer)
    Adresszusatz = Column(String)
    Gemarkung = Column(String)
    FlurFlurstuecknummern = Column(String)
    Laengengrad = Column(Float)
    Breitengrad = Column(Float)
    Registrierungsdatum = Column(String)
    Inbetriebnahmedatum = Column(String)
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(Integer)
    Einsatzverantwortlicher = Column(String)
    NameStromverbrauchseinheit = Column(String)
    AnzahlStromverbrauchseinheitenGroesser50Mw = Column(Integer)
    PraequalifiziertGemaessAblav = Column(Boolean)
    AnteilBeinflussbareLast = Column(Float)
    ArtAbschaltbareLast = Column(String)
    DatumDesBetreiberwechsels = Column(Date)
    DatumRegistrierungDesBetreiberwechsels = Column(Date)
    DatumBeginnVoruebergehendeStilllegung = Column(Date)
    DatumEndgueltigeStilllegung = Column(Date)
    GeplantesInbetriebnahmedatum = Column(Date)


class MarketRoles(ParentAllTables, Base):
    __tablename__ = "market_roles"

    MastrNummer = Column(String, primary_key=True)
    MarktakteurMastrNummer = Column(String)
    Marktrolle = Column(String)
    Marktpartneridentifikationsnummer_nv = Column(Boolean)
    BundesnetzagenturBetriebsnummer = Column(String)
    BundesnetzagenturBetriebsnummer_nv = Column(Boolean)
    Marktpartneridentifikationsnummer = Column(String)
    KontaktdatenMarktrolle = Column(String)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))


class MarketActors(ParentAllTables, Base):
    __tablename__ = "market_actors"

    MastrNummer = Column(String, primary_key=True)
    Personenart = Column(String)
    Marktfunktion = Column(String)
    RegistergerichtAusland = Column(String)
    Registernummer = Column(String)
    DatumLetzeAktualisierung = Column(DateTime(timezone=True))
    Firmenname = Column(String)
    Rechtsform = Column(String)
    Land = Column(String)
    Strasse = Column(String)
    Hausnummer = Column(String)
    Hausnummer_nv = Column(Boolean)
    Postleitzahl = Column(String)
    Ort = Column(String)
    Bundesland = Column(String)
    Nuts2 = Column(String)
    Email = Column(String)
    Telefon = Column(String)
    Fax_nv = Column(Boolean)
    Webseite_nv = Column(Boolean)
    Taetigkeitsbeginn = Column(Date)
    AcerCode_nv = Column(Boolean)
    Umsatzsteueridentifikationsnummer_nv = Column(Boolean)
    BundesnetzagenturBetriebsnummer = Column(String)
    BundesnetzagenturBetriebsnummer_nv = Column(Boolean)
    HausnummerAnZustelladresse_nv = Column(Boolean)
    Kmu = Column(Integer)
    RegistrierungsdatumMarktakteur = Column(DateTime(timezone=True))
    Fax = Column(String)
    HauptwirtdschaftszweigAbteilung = Column(String)
    HauptwirtdschaftszweigGruppe = Column(String)
    HauptwirtdschaftszweigAbschnitt = Column(String)
    Webseite = Column(String)
    Umsatzsteueridentifikationsnummer = Column(String)
    Registergericht = Column(String)
    Adresszusatz = Column(String)
    LandAnZustelladresse = Column(String)
    PostleitzahlAnZustelladresse = Column(String)
    OrtAnZustelladresse = Column(String)
    StrasseAnZustelladresse = Column(String)
    HausnummerAnZustelladresse = Column(String)
    RegisternummerAusland = Column(String)
    SonstigeRechtsform = Column(String)
    AcerCode = Column(String)
    AdresszusatzAnZustelladresse = Column(String)
    Taetigkeitsende = Column(Date)
    Region = Column(String)
    Taetigkeitsende_nv = Column(Boolean)
    Marktrollen = Column(String)
    Gasgrosshaendler = Column(Boolean)
    BelieferungVonLetztverbrauchernGas = Column(Boolean)
    BelieferungHaushaltskundenGas = Column(Boolean)
    Netz = Column(String)
    Direktvermarktungsunternehmen = Column(Boolean)
    BelieferungVonLetztverbrauchernStrom = Column(Boolean)
    BelieferungHaushaltskundenStrom = Column(Boolean)
    Stromgrosshaendler = Column(Boolean)
    MarktakteurVorname = Column(String)
    MarktakteurNachname = Column(String)
    WebportalDesNetzbetreibers = Column(String)
    RegisternummerPraefix = Column(String)


class Grids(ParentAllTables, Base):
    __tablename__ = "grids"

    MastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Sparte = Column(String)
    KundenAngeschlossen = Column(String)
    GeschlossenesVerteilnetz = Column(String)
    Bezeichnung = Column(String)
    Marktgebiet = Column(String)
    Bundesland = Column(String)


class GridConnections(ParentAllTables, Base):
    __tablename__ = "grid_connections"

    NetzanschlusspunktMastrNummer = Column(String, primary_key=True)
    NetzanschlusspunktBezeichnung = Column(String)
    LetzteAenderung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    Lokationtyp = Column(String)
    MaximaleEinspeiseleistung = Column(Float)
    Gasqualitaet = Column(String)
    NetzMastrNummer = Column(String)
    NochInPlanung = Column(Boolean)
    NameDerTechnischenLokation = Column(String)
    MaximaleAusspeiseleistung = Column(Float)
    Messlokation = Column(String)
    Spannungsebene = Column(String)
    BilanzierungsgebietNetzanschlusspunktId = Column(Integer)
    Nettoengpassleistung = Column(Float)
    Netzanschlusskapazitaet = Column(Float)


class DeletedUnits(ParentAllTables, Base):
    __tablename__ = "deleted_units"

    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    EinheitMastrNummer = Column(String, primary_key=True)
    Einheittyp = Column(String)
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)


class DeletedMarketActors(ParentAllTables, Base):
    __tablename__ = "deleted_market_actors"

    MarktakteurMastrNummer = Column(String, primary_key=True)
    MarktakteurStatus = Column(String)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))


class RetrofitUnits(ParentAllTables, Base):
    __tablename__ = "retrofit_units"

    Id = Column(Integer, primary_key=True)
    EegMastrNummer = Column(String)
    Leistungserhoehung = Column(Float)
    WiederinbetriebnahmeDatum = Column(Date)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Ertuechtigungsart = Column(String)
    ErtuechtigungIstZulassungspflichtig = Column(Boolean)


class ChangedDSOAssignment(ParentAllTables, Base):
    __tablename__ = "changed_dso_assignment"

    EinheitMastrNummer = Column(String, primary_key=True)
    LokationMastrNummer = Column(String)
    NetzanschlusspunktMastrNummer = Column(String)
    NetzbetreiberMastrNummerNeu = Column(String)
    NetzbetreiberMastrNummerAlt = Column(String)
    ArtDerAenderung = Column(String)
    RegistrierungsdatumNetzbetreiberzuordnungsaenderung = Column(
        DateTime(timezone=True)
    )
    Netzbetreiberzuordnungsaenderungsdatum = Column(DateTime(timezone=True))


tablename_mapping = {
    "anlageneegbiomasse": {
        "__name__": BiomassEeg.__tablename__,
        "__class__": BiomassEeg,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
            "BiogasHoechstbemessungsleistung": "Hoechstbemessungsleistung",
        },
    },
    "einheitenbiomasse": {
        "__name__": BiomassExtended.__tablename__,
        "__class__": BiomassExtended,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "KwkMaStRNummer": "KwkMastrNummer",
            "LokationMaStRNummer": "LokationMastrNummer",
        },
    },
    "anlageneeggeothermiegrubengasdruckentspannung": {
        "__name__": GsgkEeg.__tablename__,
        "__class__": GsgkEeg,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "einheitengeothermiegrubengasdruckentspannung": {
        "__name__": GsgkExtended.__tablename__,
        "__class__": GsgkExtended,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "KwkMaStRNummer": "KwkMastrNummer",
            "LokationMaStRNummer": "LokationMastrNummer",
        },
    },
    "anlageneegsolar": {
        "__name__": SolarEeg.__tablename__,
        "__class__": SolarEeg,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "einheitensolar": {
        "__name__": SolarExtended.__tablename__,
        "__class__": SolarExtended,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "LokationMaStRNummer": "LokationMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "anlageneegspeicher": {
        "__name__": StorageEeg.__tablename__,
        "__class__": StorageEeg,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
            "Zuschlagsnummer": "eegZuschlagsnummer",
        },
    },
    "anlageneegwasser": {
        "__name__": HydroEeg.__tablename__,
        "__class__": HydroEeg,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "einheitenwasser": {
        "__name__": HydroExtended.__tablename__,
        "__class__": HydroExtended,
        "replace_column_names": {
            "EegMaStRNummer": "EegMastrNummer",
            "LokationMaStRNummer": "LokationMastrNummer",
        },
    },
    "anlageneegwind": {
        "__name__": WindEeg.__tablename__,
        "__class__": WindEeg,
        "replace_column_names": {
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
            "EegMaStRNummer": "EegMastrNummer",
        },
    },
    "einheitenwind": {
        "__name__": WindExtended.__tablename__,
        "__class__": WindExtended,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "EegMaStRNummer": "EegMastrNummer",
            "Nachtkennzeichnung": "Nachtkennzeichen",
        },
    },
    "anlagengasspeicher": {
        "__name__": GasStorage.__tablename__,
        "__class__": GasStorage,
        "replace_column_names": {
            "MaStRNummer": "MastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "einheitengasspeicher": {
        "__name__": GasStorageExtended.__tablename__,
        "__class__": GasStorageExtended,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "SpeicherMaStRNummer": "SpeicherMastrNummer",
        },
    },
    "anlagenkwk": {
        "__name__": Kwk.__tablename__,
        "__class__": Kwk,
        "replace_column_names": {
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheiten"
        },
    },
    "anlagenstromspeicher": {
        "__name__": StorageUnits.__tablename__,
        "__class__": StorageUnits,
        "replace_column_names": {
            "MaStRNummer": "MastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "bilanzierungsgebiete": {
        "__name__": BalancingArea.__tablename__,
        "__class__": BalancingArea,
        "replace_column_names": None,
    },
    "einheitenaenderungnetzbetreiberzuordnungen": {
        "__name__": ChangedDSOAssignment.__tablename__,
        "__class__": ChangedDSOAssignment,
        "replace_column_names": None,
    },
    "einheitengaserzeuger": {
        "__name__": GasProducer.__tablename__,
        "__class__": GasProducer,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "SpeicherMaStRNummer": "SpeicherMastrNummer",
        },
    },
    "einheitengasverbraucher": {
        "__name__": GasConsumer.__tablename__,
        "__class__": GasConsumer,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheit",
        },
    },
    "einheitengenehmigung": {
        "__name__": Permit.__tablename__,
        "__class__": Permit,
        "replace_column_names": {
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheiten"
        },
    },
    "einheitenkernkraft": {
        "__name__": NuclearExtended.__tablename__,
        "__class__": NuclearExtended,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
        },
    },
    "einheitenstromverbraucher": {
        "__name__": ElectricityConsumer.__tablename__,
        "__class__": ElectricityConsumer,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
        },
    },
    "einheitenstromspeicher": {
        "__name__": StorageExtended.__tablename__,
        "__class__": StorageExtended,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "EegMaStRNummer": "EegMastrNummer",
        },
    },
    "einheitenverbrennung": {
        "__name__": CombustionExtended.__tablename__,
        "__class__": CombustionExtended,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "KwkMaStRNummer": "KwkMastrNummer",
        },
    },
    "ertuechtigungen": {
        "__name__": RetrofitUnits.__tablename__,
        "__class__": RetrofitUnits,
        "replace_column_names": None,
    },
    "geloeschteunddeaktivierteeinheiten": {
        "__name__": DeletedUnits.__tablename__,
        "__class__": DeletedUnits,
        "replace_column_names": None,
    },
    "geloeschteunddeaktiviertemarktakteure": {
        "__name__": DeletedMarketActors.__tablename__,
        "__class__": DeletedMarketActors,
        "replace_column_names": None,
    },
    "marktrollen": {
        "__name__": MarketRoles.__tablename__,
        "__class__": MarketRoles,
        "replace_column_names": None,
    },
    "marktakteure": {
        "__name__": MarketActors.__tablename__,
        "__class__": MarketActors,
        "replace_column_names": None,
    },
    "netze": {
        "__name__": Grids.__tablename__,
        "__class__": Grids,
        "replace_column_names": None,
    },
    "netzanschlusspunkte": {
        "__name__": GridConnections.__tablename__,
        "__class__": GridConnections,
        "replace_column_names": {
            "LokationMaStRNummer": "LokationMastrNummer",
            "NetzMaStRNummer": "NetzMastrNummer",
        },
    },
    "katalogkategorien": {
        "__name__": "katalogkategorien",
        "__class__": None,
        "replace_column_names": None,
    },
    "katalogwerte": {
        "__name__": "katalogwerte",
        "__class__": None,
        "replace_column_names": None,
    },
    "lokationen": {
        "__name__": LocationExtended.__tablename__,
        "__class__": LocationExtended,
        "replace_column_names": {
            "VerknuepfteEinheitenMaStRNummern": "VerknuepfteEinheiten",
            "NetzanschlusspunkteMaStRNummern": "Netzanschlusspunkte",
        },
    },
    "einheitentypen": {
        "__name__": "einheitentypen",
        "__class__": None,
        "replace_column_names": None,
    },
}
