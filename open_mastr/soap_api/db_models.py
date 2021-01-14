from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from sqlalchemy import Column, Integer, String, Float, Sequence, DateTime, Boolean, func, Date

mirror_schema = "mastr_mirrored"
meta = MetaData(schema=mirror_schema)
Base = declarative_base(metadata=meta)


class BasicUnit(Base):
    __tablename__ = "basic_units"

    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzeAktualisierung = Column(DateTime(timezone=True))
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
    StatisikFlag = Column(String)


class AdditionalDataRequested(Base):
    __tablename__ = "additional_data_requested"

    # TODO: Add foreign key constraint on EinheitMastrNummer
    id = Column(Integer, Sequence("additional_data_requested_id_seq", schema=mirror_schema), primary_key=True)
    EinheitMastrNummer = Column(String)
    additional_data_id = Column(String)
    technology = Column(String)
    data_type = Column(String)
    request_date = Column(DateTime(timezone=True), default=func.now())


class MissedAdditionalData(Base):

    __tablename__ = "missed_additional_data"

    id = Column(Integer, Sequence("additional_data_missed_id_seq", schema=mirror_schema), primary_key=True)
    additional_data_id = Column(String)
    download_date = Column(DateTime(timezone=True), default=func.now())


class Extended(object):

    EinheitMastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    LokationMastrNummer = Column(String)
    NetzbetreiberpruefungStatus = Column(String)
    NetzbetreiberpruefungDatum = Column(DateTime(timezone=True))
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
    Laengengrad = Column(String)
    Breitengrad = Column(String)
    UtmZonenwert = Column(String)
    UtmEast = Column(String)
    UtmNorth = Column(String)
    GaussKruegerHoch = Column(String)
    GaussKruegerRechts = Column(String)
    Meldedatum = Column(DateTime(timezone=True))
    GeplantesInbetriebnahmedatum = Column(DateTime(timezone=True))
    Inbetriebnahmedatum = Column(DateTime(timezone=True))
    DatumEndgueltigeStilllegung = Column(DateTime(timezone=True))
    DatumBeginnVoruebergehendeStilllegung = Column(DateTime(timezone=True))
    DatumWiederaufnahmeBetrieb = Column(DateTime(timezone=True))
    EinheitSystemstatus = Column(String)
    EinheitBetriebsstatus = Column(String)
    BestandsanlageMastrNummer = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(Boolean)
    AltAnlagenbetreiberMastrNummer = Column(String)
    DatumDesBetreiberwechsels = Column(DateTime(timezone=True))
    DatumRegistrierungDesBetreiberwechsels = Column(DateTime(timezone=True))
    StatisikFlag = Column(String)
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
    download_date = Column(DateTime(timezone=True), default=func.now())


class WindExtended(Extended, Base):
    __tablename__ = 'mastr_wind'

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
    EegMastrNummer = Column(String)


class SolarExtended(Extended, Base):
    __tablename__ = "mastr_solar"

    zugeordneteWirkleistungWechselrichter = Column(Float)
    GemeinsamerWechselrichterMitSpeicher = Column(String)
    AnzahlModule = Column(Integer)
    Lage = Column(String)
    Leistungsbegrenzung = Column(String)
    EinheitlicheAusrichtungUndNeigungswinkel = Column(Boolean)
    Hauptausrichtung = Column(String)
    HauptausrichtungNeigungswinkel = Column(String)
    Nebenausrichtung = Column(String)
    NebenausrichtungNeigungswinkel = Column(String)
    InAnspruchGenommeneFlaeche = Column(String)
    ArtDerFlaeche = Column(String)
    InAnspruchGenommeneAckerflaeche = Column(Float)
    Nutzungsbereich = Column(String)
    EegMastrNummer = Column(String)


class BiomassExtended(Extended, Base):
    __tablename__ = "biomass_extended"

    Hauptbrennstoff = Column(String)
    Biomasseart = Column(String)
    Technologie = Column(String)
    EegMastrNummer = Column(String)
    KwkMastrNummer = Column(String)


class Eeg(object):
    EegMastrNummer = Column(String, primary_key=True)
    Meldedatum = Column(Date)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    EegInbetriebnahmedatum = Column(Date)
    AnlagenkennzifferAnlagenregister = Column(String)
    AnlagenschluesselEeg = Column(String)
    PrototypAnlage = Column(Boolean)
    PilotAnlage = Column(Boolean)
    InstallierteLeistung = Column(Float)
    AusschreibungZuschlag = Column(Boolean)
    Zuschlagsnummer = Column(String)
    AnlageBetriebsstatus = Column(String)
    VerknuepfteEinheit = Column(String)


class WindEeg(Eeg, Base):
    __tablename__ = "wind_eeg"

    VerhaeltnisErtragsschaetzungReferenzertrag = Column(Float)
    VerhaeltnisReferenzertragErtrag5Jahre = Column(Float)
    VerhaeltnisReferenzertragErtrag10Jahre = Column(Float)
    VerhaeltnisReferenzertragErtrag15Jahre = Column(Float)


class SolarEeg(Eeg, Base):
    __tablename__ = "solar_eeg"

    InanspruchnahmeZahlungNachEeg = Column(Boolean)
    RegistrierungsnummerPvMeldeportal = Column(String)
    MieterstromZugeordnet = Column(Boolean)
    MieterstromMeldedatum = Column(Date)
    MieterstromErsteZuordnungZuschlag = Column(Date)
    ZugeordneteGebotsmenge = Column(Float)


class BiomassEeg(Eeg, Base):
    __tablename__ = "biomass_eeg"

    AusschliesslicheVerwendungBiomasse = Column(Boolean)
    BiogasInanspruchnahmeFlexiPraemie = Column(Boolean)
    BiogasDatumInanspruchnahmeFlexiPraemie = Column(Date)
    BiogasLeistungserhoehung = Column(Boolean)
    BiogasDatumLeistungserhoehung = Column(Date)
    BiogasUmfangLeistungserhoehung = Column(Float)
    BiogasGaserzeugungskapazitaet = Column(Float)
    BiogasHoechstbemessungsleistung = Column(Float)
    BiomethanErstmaligerEinsatz = Column(Date)


class Permit(Base):
    __tablename__ = "permit"

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
