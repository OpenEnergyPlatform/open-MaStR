#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Wind

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from soap_api.sessions import mastr_session
from soap_api.utils import write_to_csv, get_data_version, read_power_units
from soap_api.utils import fname_power_unit, fname_power_unit_wind, fname_wind_unit, fname_wind_eeg, fname_wind_permit

import pandas as pd
import datetime
import os
from zeep.helpers import serialize_object

import logging

log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def setup_power_unit_wind():
    """Setup file for Stromerzeugungseinheit-Wind.

    Check if file with Stromerzeugungseinheit exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Wind.
    Remove duplicates and write to file.

    Returns
    -------
    power_unit_wind : DataFrame
        Stromerzeugungseinheit-Wind.
    """

    if os.path.isfile(fname_power_unit_wind):
        log.info(f'Skip setup for Stromerzeugungseinheit-Wind')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)
            power_unit_cnt = power_unit['timestamp'].count()
            log.info(f'Read {power_unit_cnt} power-unit from {fname_power_unit}')

            power_unit_wind = power_unit[power_unit.Einheittyp == 'Windeinheit']
            power_unit_wind = power_unit_wind.drop_duplicates(subset=['EinheitMastrNummer',
                                                                      'Name',
                                                                      'Einheitart',
                                                                      'Einheittyp',
                                                                      'Standort',
                                                                      'Bruttoleistung',
                                                                      'Erzeugungsleistung',
                                                                      'EinheitBetriebsstatus',
                                                                      'Anlagenbetreiber',
                                                                      'EegMastrNummer',
                                                                      'KwkMastrNummer',
                                                                      'SpeMastrNummer',
                                                                      'GenMastrNummer'])
            log.info(f'Filter power-unit for wind and remove duplicates')
            power_unit_wind.index.names = ['see_id']
            power_unit_wind.reset_index()
            power_unit_wind.index.names = ['id']

            write_to_csv(fname_power_unit_wind, power_unit_wind)
            power_unit_wind_cnt = power_unit_wind['timestamp'].count()
            log.info(f'Write {power_unit_wind_cnt} power-unit_wind to {fname_power_unit_wind}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_wind(csv_name):
    """Read Stromerzeugungseinheit-Wind from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_wind : DataFrame
        Stromerzeugungseinheit-Wind.
    """

    if os.path.isfile(csv_name):
        power_unit_wind = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                                      dtype={
                                          'id': str,
                                          'lid': str,
                                          'EinheitMastrNummer': str,
                                          'Name': str,
                                          'Einheitart': str,
                                          'Einheittyp': str,
                                          'Standort': str,
                                          'Bruttoleistung': str,
                                          'Erzeugungsleistung': str,
                                          'EinheitBetriebsstatus': str,
                                          'Anlagenbetreiber': str,
                                          'EegMastrNummer': str,
                                          'KwkMastrNummer': str,
                                          'SpeMastrNummer': str,
                                          'GenMastrNummer': str,
                                          'BestandsanlageMastrNummer': str,
                                          'NichtVorhandenInMigriertenEinheiten': str,
                                          'StatisikFlag': str,
                                          'version': str,
                                          'timestamp': str})
        power_unit_wind_cnt = power_unit_wind['id'].count()
        log.info(f'Read {power_unit_wind_cnt} power-unit_wind from {csv_name}')
        return power_unit_wind

    else:
        log.info(f'Error reading {csv_name}')


def get_power_unit_wind(mastr_unit_wind):
    """Get Windeinheit from API using GetEinheitWind.

    Parameters
    ----------
    mastr_unit_wind : object
        Wind from EinheitMastrNummerId.

    Returns
    -------
    unit_wind : DataFrame
        Windeinheit.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitWind(apiKey=api_key,
                                       marktakteurMastrNummer=my_mastr,
                                       einheitMastrNummer=mastr_unit_wind)
        c = disentangle_manufacturer(c)
        del c['Hersteller']
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_wind = df.set_index(list(df.columns.values)[0]).transpose()
        unit_wind.reset_index()
        unit_wind.index.names = ['lid']
        unit_wind['version'] = data_version
        unit_wind['timestamp'] = str(datetime.datetime.now())
    except Exception as e:
        log.info('Download failed for %s', mastr_unit_wind)
    return unit_wind


def read_unit_wind(csv_name):
    """Read Windeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind : DataFrame
        Windeinheit.
    """
    # log.info(f'Read data from {csv_name}')
    unit_wind = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                            dtype={'lid': int,
                                   'Ergebniscode': str,
                                   'AufrufVeraltet': str,
                                   'AufrufLebenszeitEnde': str,
                                   'AufrufVersion': str,
                                   'EinheitMastrNummer': str,
                                   'DatumLetzteAktualisierung': str,
                                   'LokationMastrNummer': str,
                                   'NetzbetreiberpruefungStatus': str,
                                   'NetzbetreiberpruefungDatum': str,
                                   'AnlagenbetreiberMastrNummer': str,
                                   'Land': str,
                                   'Bundesland': str,
                                   'Landkreis': str,
                                   'Gemeinde': str,
                                   'Gemeindeschluessel': str,
                                   'Postleitzahl': str,
                                   'Gemarkung': str,
                                   'FlurFlurstuecknummern': str,
                                   'Strasse': str,
                                   'StrasseNichtGefunden': str,
                                   'Hausnummer': str,
                                   'HausnummerNichtGefunden': str,
                                   'Adresszusatz': str,
                                   'Ort': str,
                                   'Laengengrad': str,
                                   'Breitengrad': str,
                                   'UtmZonenwert': str,
                                   'UtmEast': str,
                                   'UtmNorth': str,
                                   'GaussKruegerHoch': str,
                                   'GaussKruegerRechts': str,
                                   'Meldedatum': str,
                                   'GeplantesInbetriebnahmedatum': str,
                                   'Inbetriebnahmedatum': str,
                                   'DatumEndgueltigeStilllegung': str,
                                   'DatumBeginnVoruebergehendeStilllegung': str,
                                   'DatumWiederaufnahmeBetrieb': str,
                                   'EinheitBetriebsstatus': str,
                                   'BestandsanlageMastrNummer': str,
                                   'NichtVorhandenInMigriertenEinheiten': str,
                                   'NameStromerzeugungseinheit': str,
                                   'Weic': str,
                                   'WeicDisplayName': str,
                                   'Kraftwerksnummer': str,
                                   'Energietraeger': str,
                                   'Bruttoleistung': float,
                                   'Nettonennleistung': float,
                                   'AnschlussAnHoechstOderHochSpannung': str,
                                   'Schwarzstartfaehigkeit': str,
                                   'Inselbetriebsfaehigkeit': str,
                                   'Einsatzverantwortlicher': str,
                                   'FernsteuerbarkeitNb': str,
                                   'FernsteuerbarkeitDv': str,
                                   'FernsteuerbarkeitDr': str,
                                   'Einspeisungsart': str,
                                   'PraequalifiziertFuerRegelenergie': str,
                                   'GenMastrNummer': str,
                                   'NameWindpark': str,
                                   'Lage': str,
                                   'Seelage': str,
                                   'ClusterOstsee': str,
                                   'ClusterNordsee': str,
                                   'Technologie': str,
                                   'Typenbezeichnung': str,
                                   'Nabenhoehe': float,
                                   'Rotordurchmesser': float,
                                   'AuflageAbschaltungLeistungsbegrenzung': str,
                                   'Wassertiefe': float,
                                   'Kuestenentfernung': float,
                                   'EegMastrNummer': str,
                                   'HerstellerID': str,
                                   'HerstellerName': str,
                                   'version': str,
                                   'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_wind


def get_unit_wind_eeg(mastr_wind_eeg):
    """Get EEG-Anlage-Wind from API using GetAnlageEegWind.

    Parameters
    ----------
    mastr_wind_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_wind_eeg : DataFrame
        EEG-Anlage-Wind.
    """
    data_version = get_data_version()
    c = client_bind.GetAnlageEegWind(apiKey=api_key,
                                     marktakteurMastrNummer=my_mastr,
                                     eegMastrNummer=mastr_wind_eeg)
    # c['VerknuepfteEinheit'] = c['MaStR']['VerknuepfteEinheit']
    # del c['MaStR']
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_wind_eeg = df.set_index(list(df.columns.values)[0]).transpose()
    unit_wind_eeg.reset_index()
    unit_wind_eeg.index.names = ['lid']
    unit_wind_eeg["version"] = data_version
    unit_wind_eeg["timestamp"] = str(datetime.datetime.now())
    return unit_wind_eeg


def read_unit_wind_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Wind from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind_eeg : DataFrame
        EEG-Anlage-Wind
    """
    # log.info(f'Read data from {csv_name}')
    unit_wind_eeg = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                dtype={'lid': int,
                                       'Ergebniscode': str,
                                       'AufrufVeraltet': str,
                                       'AufrufLebenszeitEnde': str,
                                       'AufrufVersion': str,
                                       'Meldedatum': str,
                                       'DatumLetzteAktualisierung': str,
                                       'EegInbetriebnahmedatum': str,
                                       'EegMastrNummer': str,
                                       'AnlagenkennzifferAnlagenregister': str,
                                       'AnlagenschluesselEeg': str,
                                       'PrototypAnlage': str,
                                       'PilotAnlage': str,
                                       'InstallierteLeistung': float,
                                       'VerhaeltnisErtragsschaetzungReferenzertrag': str,
                                       'VerhaeltnisReferenzertragErtrag5Jahre': str,
                                       'VerhaeltnisReferenzertragErtrag10Jahre': str,
                                       'VerhaeltnisReferenzertragErtrag15Jahre': str,
                                       'AusschreibungZuschlag': str,
                                       'Zuschlagsnummer': str,
                                       'AnlageBetriebsstatus': str,
                                       'VerknuepfteEinheit': str,
                                       'version': str,
                                       'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_wind_eeg


def get_unit_wind_permit(mastr_wind_permit):
    """Get Genehmigung-Wind-Wind from API using GetEinheitGenehmigung.

    Parameters
    ----------
    mastr_wind_permit : str
        Genehmigungnummer MaStR

    Returns
    -------
    unit_wind_permit : DataFrame
        Genehmigung-Einheit-Wind.
    """
    data_version = get_data_version()
    c = client_bind.GetEinheitGenehmigung(apiKey=api_key,
                                          marktakteurMastrNummer=my_mastr,
                                          genMastrNummer=mastr_wind_permit)
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_wind_permit = df.set_index(list(df.columns.values)[0]).transpose()
    unit_wind_permit.reset_index()
    unit_wind_permit.index.names = ['lid']
    unit_wind_permit["version"] = data_version
    unit_wind_permit["timestamp"] = str(datetime.datetime.now())
    unit_wind_permit = unit_wind_permit.replace('\r', '', regex=True)
    return unit_wind_permit


def read_unit_wind_permit(csv_name):
    """
    Encode and read Genehmigung-Einheit-Wind from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind_permit : DataFrame
        Genehmigung-Einheit-Wind
    """
    # log.info(f'Read data from {csv_name}')
    unit_wind_permit = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                   dtype={'lid': int,
                                          'EinheitMastrNummer': str,
                                          'Einheittyp': str,
                                          'Einheitart': str,
                                          'GenMastrNummer': str,
                                          'Datum': str,
                                          'Art': str,
                                          'Behoerde': str,
                                          'Aktenzeichen': str,
                                          'Frist': str,
                                          'WasserrechtsNummer': str,
                                          'WasserrechtAblaufdatum': str,
                                          'Meldedatum': str
                                          })
    # log.info(f'Finished reading data from {csv_name}')
    return unit_wind_permit


def download_unit_wind():
    """Download Windeinheit. Write results to csv file.

    Existing units: 31543 (2019-02-10)

    Parameters
    ----------


    Returns
    -------

    """
    start_from = 0
    setup_power_unit_wind()
    power_unit_wind = read_power_unit_wind(fname_power_unit_wind)
    power_unit_wind = power_unit_wind['EinheitMastrNummer']
    power_unit_wind_list = power_unit_wind.values.tolist()
    power_unit_wind_list_len = len(power_unit_wind_list)
    log.info(f'Download {power_unit_wind_list_len} Windeinheit')

    for i in range(start_from, power_unit_wind_list_len, 1):
        try:
            unit_wind = get_power_unit_wind(power_unit_wind_list[i])
            write_to_csv(fname_wind_unit, unit_wind)
        except:
            log.exception(f'Download failed unit_wind ({i}): {unit_wind_list[i]}')


def download_unit_wind_eeg():
    """Download unit_wind_eeg using GetAnlageEegWind request.

    Parameters
    ----------


    Returns
    -------

    """
    setup_power_unit_wind()
    power_unit_wind = read_power_unit_wind(fname_power_unit_wind)
    power_unit_wind = power_unit_wind['EegMastrNummer']
    power_unit_wind_list = power_unit_wind.values.tolist()
    power_unit_wind_list_len = len(power_unit_wind_list)
    log.info(f'Download {power_unit_wind_list_len} Windeinheit EEG')

    for i in range(0, power_unit_wind_list_len, 1):
        try:
            unit_wind_eeg = get_unit_wind_eeg(power_unit_wind_list[i])
            write_to_csv(fname_wind_eeg, unit_wind_eeg)
        except:
            log.exception(f'Download failed unit_wind_eeg ({i}): {power_unit_wind_list[i]}')


def download_unit_wind_permit():
    """Download unit_wind_permit using GetEinheitGenehmigung request.

    ToDo: More Documentation needed @solar-c

    Parameters
    ----------


    Returns
    -------

    """
    setup_power_unit_wind()
    power_unit_wind = read_power_unit_wind(fname_power_unit_wind)
    power_unit_wind = power_unit_wind['GenMastrNummer'].drop_duplicates
    power_unit_wind_list = power_unit_wind.values.tolist()
    power_unit_wind_list_len = len(power_unit_wind_list)
    log.info(f'Download {power_unit_wind_list_len} Windeinheit Permit')

    df_all = pd.DataFrame()

    for i in range(0, power_unit_wind_list_len, 1):
        if not pd.isna(power_unit_wind_list[i]):
            try:
                unit_wind_permit = get_unit_wind_permit(power_unit_wind_list[i])
                for k, v in unit_wind_permit.VerknuepfteEinheiten.items():
                    df_new = pd.DataFrame.from_dict(v)
                    df = pd.DataFrame()
                    gennr = df_new.size * [unit_wind_permit.GenMastrNummer.iloc[0]]
                    dates = df_new.size * [unit_wind_permit.Datum.iloc[0]]
                    types = df_new.size * [unit_wind_permit.Art.iloc[0]]
                    authority = df_new.size * [(unit_wind_permit.Behoerde.iloc[0]).translate({ord(','): None})]
                    file_num = df_new.size * [unit_wind_permit.Aktenzeichen.iloc[0]]
                    frist = df_new.size * [unit_wind_permit.Frist.iloc[0]['Wert']]
                    water_num = df_new.size * [unit_wind_permit.WasserrechtsNummer.iloc[0]]
                    water_date = df_new.size * [unit_wind_permit.WasserrechtAblaufdatum.iloc[0]['Wert']]
                    reporting_date = df_new.size * [unit_wind_permit.Meldedatum.iloc[0]]
                    df = pd.DataFrame(
                        {
                            'GenMastrNummer': gennr,
                            'Datum': dates,
                            'Art': types,
                            'Behoerde': authority,
                            'Aktenzeichen': file_num,
                            'Frist': frist,
                            'WasserrechtsNummer': water_num,
                            'WasserrechtAblaufdatum': water_date,
                            'Meldedatum': reporting_date
                        })
                    df_all = pd.concat([df_new, df.reindex(df_new.index)], axis=1)
                    # df_all.set_index(['MaStRNummer'], inplace=True)
                    write_to_csv(fname_wind_permit, df_all)
            except:
                log.exception(f'Download failed unit_wind_permit ({i}): {power_unit_wind_list[i]}')


def disentangle_manufacturer(wind_unit):
    """

    Parameters
    ----------
    wind_unit

    Returns
    -------

    """
    wu = wind_unit
    try:
        wu['HerstellerID'] = wind_unit['Hersteller']['Id']
        wu['HerstellerName'] = wind_unit['Hersteller']['Wert']
        return (wu)
    except:
        print("This wind_unit contains no OrderedDict for 'Hersteller'")
        return (wind_unit)
