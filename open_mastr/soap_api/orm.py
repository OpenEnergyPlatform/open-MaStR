from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from sqlalchemy import Column, Integer, String, Float, Sequence, DateTime, Boolean, func, Date, JSON

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
    reason = Column(String)
    download_date = Column(DateTime(timezone=True), default=func.now())


class Extended(object):

    NetzbetreiberMastrNummer = Column(String)
    Registrierungsdatum = Column(Date)
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
    __tablename__ = 'wind_extended'

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
    __tablename__ = "solar_extended"

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


class CombustionExtended(Extended, Base):
    __tablename__ = "combustion_extended"

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
    KwkMastrNummer = Column(String)
    Technologie = Column(String)


class GsgkExtended(Extended, Base):
    __tablename__ = "gsgk_extended"

    Technologie = Column(String)
    KwkMastrNummer = Column(String)
    EegMastrNummer = Column(String)


class HydroExtended(Extended, Base):
    __tablename__ = "hydro_extended"

    NameKraftwerk = Column(String)
    ArtDerWasserkraftanlage = Column(String)
    AnzeigeEinerStilllegung = Column(Boolean)
    ArtDerStilllegung = Column(String)
    DatumBeginnVorlaeufigenOderEndgueltigenStilllegung = Column(Date)
    MinderungStromerzeugung = Column(Boolean)
    BestandteilGrenzkraftwerk = Column(Boolean)
    NettonennleistungDeutschland = Column(Float)
    ArtDesZuflusses = Column(String)
    EegMastrNummer = Column(String)


class NuclearExtended(Extended, Base):
    __tablename__ = "nuclear_extended"

    NameKraftwerk = Column(String)
    NameKraftwerksblock = Column(String)
    Technologie = Column(String)


class StorageExtended(Extended, Base):
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
    ZugeordnenteWirkleistungWechselrichter = Column(Float)
    NutzbareSpeicherkapazitaet = Column(Float)
    SpeMastrNummer = Column(String)
    EegMastrNummer = Column(String)
    EegAnlagentyp = Column(String)
    Technologie = Column(String)


class Eeg(object):
    EegMastrNummer = Column(String, primary_key=True)
    Meldedatum = Column(Date)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    EegInbetriebnahmedatum = Column(Date)
    VerknuepfteEinheit = Column(String)


class WindEeg(Eeg, Base):
    __tablename__ = "wind_eeg"

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


class SolarEeg(Eeg, Base):
    __tablename__ = "solar_eeg"

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


class BiomassEeg(Eeg, Base):
    __tablename__ = "biomass_eeg"

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


class GsgkEeg(Eeg, Base):
    __tablename__ = "gsgk_eeg"

    AnlagenschluesselEeg = Column(String)
    AnlagenkennzifferAnlagenregister = Column(String)
    InstallierteLeistung = Column(Float)
    AnlageBetriebsstatus = Column(String)


class HydroEeg(Eeg, Base):
    __tablename__ = "hydro_eeg"

    AnlagenschluesselEeg = Column(String)
    AnlagenkennzifferAnlagenregister = Column(String)
    InstallierteLeistung = Column(Float)
    AnlageBetriebsstatus = Column(String)
    Ertuechtigung = Column(JSON)


class StorageEeg(Eeg, Base):
    __tablename__ = "storage_eeg"


class Kwk(Base):
    __tablename__ = "kwk"

    KwkMastrNummer = Column(String, primary_key=True)
    AusschreibungZuschlag = Column(Boolean)
    Zuschlagnummer = Column(String)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    Inbetriebnahmedatum = Column(Date)
    Meldedatum = Column(Date)
    ThermischeNutzleistung = Column(Float)
    ElektrischeKwkLeistung = Column(Float)
    VerknuepfteEinheiten = Column(String)
    AnlageBetriebsstatus = Column(String)


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


class LocationBasic(Base):
    __tablename__ = "locations_basic"

    LokationMastrNummer = Column(String, primary_key=True)
    NameDerTechnischenLokation = Column(String)
    Lokationtyp = Column(String)
    AnzahlNetzanschlusspunkte = Column(Integer)


class LocationExtended(Base):
    __tablename__ = "locations_extended"

    MastrNummer = Column(String, primary_key=True)
    DatumLetzteAktualisierung = Column(DateTime(timezone=True))
    NameDerTechnischenLokation = Column(String)
    VerknuepfteEinheiten = Column(JSONB)
    Netzanschlusspunkte = Column(JSONB)


class AdditionalLocationsRequested(Base):
    __tablename__ = "additional_locations_requested"

    id = Column(Integer, Sequence("additional_locations_requested_id_seq", schema=mirror_schema), primary_key=True)
    LokationMastrNummer = Column(String)
    location_type = Column(String)
    request_date = Column(DateTime(timezone=True), default=func.now())


class MissedExtendedLocation(Base):
    __tablename__ = "missed_extended_location_data"

    id = Column(Integer, Sequence("additional_location_data_missed_id_seq", schema=mirror_schema), primary_key=True)
    LokationMastrNummer = Column(String)
    reason = Column(String)
    download_date = Column(DateTime(timezone=True), default=func.now())
