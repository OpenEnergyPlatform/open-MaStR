from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
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
from sqlalchemy.dialects.postgresql import JSONB

cleaned_schema = "model_draft"
meta = MetaData(schema=cleaned_schema)
Base = declarative_base(metadata=meta)


class BasicUnit(object):

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
    BestandsanlageMastrNummer_basic = Column(String)
    NichtVorhandenInMigriertenEinheiten = Column(String)
    StatisikFlag_basic = Column(String)


class Extended(object):

    EinheitMastrNummer_extended = Column(String)
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
    EinheitBetriebsstatus_extended = Column(String)
    BestandsanlageMastrNummer = Column(String)
    NichtVorhandenInMigriertenEinheiten_extended = Column(Boolean)
    AltAnlagenbetreiberMastrNummer = Column(String)
    DatumDesBetreiberwechsels = Column(DateTime(timezone=True))
    DatumRegistrierungDesBetreiberwechsels = Column(DateTime(timezone=True))
    StatisikFlag = Column(String)
    NameStromerzeugungseinheit = Column(String)
    Weic = Column(String)
    WeicDisplayName = Column(String)
    Kraftwerksnummer = Column(String)
    Energietraeger = Column(String)
    Bruttoleistung_extended = Column(Float)
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
    GenMastrNummer_extended = Column(String)
    geom = Column(Geometry("POINT"))
    comment = Column(String)


class Eeg(object):

    EegMastrNummer_eeg = Column(String)
    Meldedatum_eeg = Column(Date)
    DatumLetzteAktualisierung_eeg = Column(DateTime(timezone=True))
    EegInbetriebnahmedatum = Column(Date)
    VerknuepfteEinheit = Column(String)


class WindEeg(Eeg):

    AnlagenkennzifferAnlagenregister = Column(String)
    AnlagenschluesselEeg = Column(String)
    PrototypAnlage = Column(Boolean)
    PilotAnlage = Column(Boolean)
    InstallierteLeistung = Column(Float)
    VerhaeltnisErtragsschaetzungReferenzertrag = Column(Float)
    VerhaeltnisReferenzertragErtrag5Jahre = Column(Float)
    VerhaeltnisReferenzertragErtrag10Jahre = Column(Float)
    VerhaeltnisReferenzertragErtrag15Jahre = Column(Float)
    AusschreibungZuschlag = Column(Boolean)
    Zuschlagsnummer = Column(String)
    AnlageBetriebsstatus = Column(String)


class SolarEeg(Eeg):

    InanspruchnahmeZahlungNachEeg = Column(Boolean)
    AnlagenschluesselEeg = Column(String)
    AnlagenkennzifferAnlagenregister = Column(String)
    InstallierteLeistung = Column(Float)
    RegistrierungsnummerPvMeldeportal = Column(String)
    MieterstromZugeordnet = Column(Boolean)
    MieterstromMeldedatum = Column(Date)
    MieterstromErsteZuordnungZuschlag = Column(Date)
    AusschreibungZuschlag = Column(Boolean)
    ZugeordneteGebotsmenge = Column(Float)
    Zuschlagsnummer = Column(String)
    AnlageBetriebsstatus = Column(String)


class BiomassEeg(Eeg):

    AnlagenschluesselEeg = Column(String)
    AnlagenkennzifferAnlagenregister = Column(String)
    InstallierteLeistung = Column(Float)
    AusschliesslicheVerwendungBiomasse = Column(Boolean)
    AusschreibungZuschlag = Column(Boolean)
    Zuschlagsnummer = Column(String)
    BiogasInanspruchnahmeFlexiPraemie = Column(Boolean)
    BiogasDatumInanspruchnahmeFlexiPraemie = Column(Date)
    BiogasLeistungserhoehung = Column(Boolean)
    BiogasDatumLeistungserhoehung = Column(Date)
    BiogasUmfangLeistungserhoehung = Column(Float)
    BiogasGaserzeugungskapazitaet = Column(Float)
    BiogasHoechstbemessungsleistung = Column(Float)
    BiomethanErstmaligerEinsatz = Column(Date)
    AnlageBetriebsstatus = Column(String)


class GsgkEeg(Eeg):

    AnlagenschluesselEeg = Column(String)
    AnlagenkennzifferAnlagenregister = Column(String)
    InstallierteLeistung = Column(Float)
    AnlageBetriebsstatus = Column(String)


class HydroEeg(Eeg):

    AnlagenschluesselEeg = Column(String)
    AnlagenkennzifferAnlagenregister = Column(String)
    InstallierteLeistung = Column(Float)
    AnlageBetriebsstatus = Column(String)
    Ertuechtigung = Column(JSON)


class StorageEeg(Eeg):
    pass


class Kwk(object):

    KwkMastrNummer_kwk = Column(String)
    AusschreibungZuschlag_kwk = Column(Boolean)
    Zuschlagnummer = Column(String)
    DatumLetzteAktualisierung_kwk = Column(DateTime(timezone=True))
    Inbetriebnahmedatum_kwk = Column(Date)
    Meldedatum_kwk = Column(Date)
    ThermischeNutzleistung = Column(Float)
    ElektrischeKwkLeistung = Column(Float)
    VerknuepfteEinheiten_kwk = Column(String)
    AnlageBetriebsstatus_kwk = Column(String)


class Permit(object):

    GenMastrNummer_permit = Column(String)
    DatumLetzteAktualisierung_permit = Column(DateTime(timezone=True))
    Art = Column(String)
    Datum = Column(Date)
    Behoerde = Column(String)
    Aktenzeichen = Column(String)
    Frist = Column(Date)
    WasserrechtsNummer = Column(String)
    WasserrechtAblaufdatum = Column(Date)
    Meldedatum_permit = Column(Date)
    VerknuepfteEinheiten = Column(String)


class WindCleaned(Permit, WindEeg, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_wind_clean"

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
    EegMastrNummer_extended = Column(String)
    tags = Column(JSONB)
    geom_3035 = Column(Geometry("POINT", srid=3035))


class SolarCleaned(Permit, SolarEeg, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_solar_clean"

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
    EegMastrNummer_extended = Column(String)


class BiomassCleaned(Permit, Kwk, BiomassEeg, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_biomass_clean"

    Hauptbrennstoff = Column(String)
    Biomasseart = Column(String)
    Technologie = Column(String)
    EegMastrNummer_extended = Column(String)
    KwkMastrNummer_extended = Column(String)


class CombustionCleaned(Permit, Kwk, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_combustion_clean"

    NameKraftwerk = Column(String)
    NameKraftwerksblock = Column(String)
    DatumBaubeginn = Column(Date)
    AnzeigeEinerStilllegung = Column(Boolean)
    ArtDerStilllegung = Column(String)
    DatumBeginnVorlaeufigenOderEndgueltigenStilllegung = Column(Date)
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
    KwkMastrNummer_extended = Column(String)  # changed here
    Technologie = Column(String)


class GsgkCleaned(Permit, Kwk, GsgkEeg, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_gsgk_clean"

    Technologie = Column(String)
    KwkMastrNummer_extended = Column(String)
    EegMastrNummer_extended = Column(String)


class HydroCleaned(Permit, HydroEeg, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_hydro_clean"

    NameKraftwerk = Column(String)
    ArtDerWasserkraftanlage = Column(String)
    AnzeigeEinerStilllegung = Column(Boolean)
    ArtDerStilllegung = Column(String)
    DatumBeginnVorlaeufigenOderEndgueltigenStilllegung = Column(Date)
    MinderungStromerzeugung = Column(Boolean)
    BestandteilGrenzkraftwerk = Column(Boolean)
    NettonennleistungDeutschland = Column(Float)
    ArtDesZuflusses = Column(String)
    EegMastrNummer_extended = Column(String)


class NuclearCleaned(Permit, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_nuclear_clean"

    NameKraftwerk = Column(String)
    NameKraftwerksblock = Column(String)
    Technologie = Column(String)


class StorageCleaned(Permit, StorageEeg, Extended, BasicUnit, Base):
    __tablename__ = "bnetza_mastr_storage_clean"

    Einsatzort = Column(String)
    AcDcKoppelung = Column(String)
    Batterietechnologie = Column(String)
    PumpbetriebLeistungsaufnahme = Column(Float)
    PumpbetriebKontinuierlichRegelbar = Column(Boolean)
    Pumpspeichertechnologie = Column(String)
    Notstromaggregat = Column(Boolean)
    BestandteilGrenzkraftwerk = Column(Boolean)
    NettonennleistungDeutschland = Column(Float)
    ZugeordnenteWirkleistungWechselrichter = Column(Float)
    NutzbareSpeicherkapazitaet = Column(Float)
    SpeMastrNummer_extended = Column(String)
    EegMastrNummer_extended = Column(String)
    EegAnlagentyp = Column(String)
    Technologie = Column(String)
