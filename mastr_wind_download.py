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
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.5.0"

from config import get_data_version
from sessions import mastr_session

import pandas as pd
import numpy as np
import datetime
import os
from zeep.helpers import serialize_object
import codecs

"""logging"""
# log  TODO: logger not available in submodules. Please fix!
# log = logging.getLogger('RootLogger')

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit(start_from):
    """Get Stromerzeugungseinheit from API using GetGefilterteListeStromErzeuger.

    Parameters
    ----------
    start_from : int
        Skip first entries.
    """
    data_version = get_data_version()
    status = 'InBetrieb'
    c = client_bind.GetGefilterteListeStromErzeuger(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        einheitBetriebsstatus=status,
        startAb=start_from,
        limit=2000)  # Limit of API.
    s = serialize_object(c)
    power_unit = pd.DataFrame(s['Einheiten'])
    power_unit.index.names = ['lid']
    power_unit['version'] = data_version
    power_unit['timestamp'] = str(datetime.datetime.now())

    # remove double quotes from column
    power_unit['Standort'] = power_unit['Standort'].str.replace('"', '')

    return power_unit


def enc_read_power_units(csv_name):
    """Encode and read Stromerzeugungseinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit : DataFrame
        Stromerzeugungseinheit.
    """
    # log.info(f'Encode and read data from {csv_name}')
    print(f'Encode and read data from {csv_name}.')
    with codecs.open(csv_name, mode='r', encoding='utf-8', errors='replace') as enc_data:
        power_unit = pd.read_csv(enc_data, header=0, sep=';', index_col=False,
                                 dtype={'lid': str,
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
                                        'version': str,
                                        'timestamp': str})

        # log.info(f'Read data from {csv_name}')
        print(f'Read data from {csv_name}.')
    return power_unit


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
    c = client_bind.GetEinheitWind(apiKey=api_key,
                                   marktakteurMastrNummer=my_mastr,
                                   einheitMastrNummer=mastr_unit_wind)
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_wind = df.set_index(list(df.columns.values)[0]).transpose()
    unit_wind.reset_index()
    unit_wind.index.names = ['lid']
    unit_wind['version'] = data_version
    unit_wind['timestamp'] = str(datetime.datetime.now())
    return unit_wind


def enc_read_unit_wind(csv_name):
    """Encode and read Windeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind : DataFrame
        Windeinheit.
    """
    # log.info(f'Encode and read data from {csv_name}')
    print(f'Encode and read data from {csv_name}.')
    with codecs.open(csv_name, mode='r', encoding='utf-8', errors='replace') as enc_data:
        unit_wind = pd.read_csv(enc_data, header=0, sep=';', index_col=False,
                                dtype={'lid': int,
                                       'Ergebniscode': str,
                                       'AufrufVeraltet': np.bool,
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
                                       'HausnummerNichtGefunden': np.bool,
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
                                       'Hersteller': str,
                                       'Technologie': str,
                                       'Typenbezeichnung': str,
                                       'Nabenhoehe': float,
                                       'Rotordurchmesser': float,
                                       'AuflageAbschaltungLeistungsbegrenzung': str,
                                       'Wassertiefe': float,
                                       'Kuestenentfernung': float,
                                       'EegMastrNummer': str,
                                       'version': str,
                                       'timestamp': str})
        # log.info(f'Read data from {csv_name}')
        print(f'Read data from {csv_name}.')
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
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_wind_eeg = df.set_index(list(df.columns.values)[0]).transpose()
    unit_wind_eeg.reset_index()
    unit_wind_eeg.index.names = ['lid']
    unit_wind_eeg["version"] = data_version
    unit_wind_eeg["timestamp"] = str(datetime.datetime.now())
    return unit_wind_eeg


# Encode & Read MaStR Wind EEG from CSV
def enc_read_unit_wind_eeg(csv_name):
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
    # log.info(f'Encode and read data from {csv_name}')
    print(f'Encode and read data from {csv_name}.')
    with codecs.open(csv_name, mode='r', encoding='utf-8', errors='replace') as enc_data:
        unit_wind_eeg = pd.read_csv(enc_data, header=0, sep=';', index_col=False,
                                    dtype={'lid': int,
                                           'Ergebniscode': str,
                                           'AufrufVeraltet': np.bool,
                                           'AufrufLebenszeitEnde': str,
                                           'AufrufVersion': str,
                                           'Meldedatum': str,
                                           'DatumLetzteAktualisierung': str,
                                           'EegInbetriebnahmedatum': str,
                                           'EegMastrNummer': str,
                                           'AnlagenkennzifferAnlagenregister': str,
                                           'AnlagenschluesselEeg': str,
                                           'PrototypAnlage': np.bool,
                                           'PilotAnlage': np.bool,
                                           'InstallierteLeistung': float,
                                           'VerhaeltnisErtragsschaetzungReferenzertrag': str,
                                           'VerhaeltnisReferenzertragErtrag5Jahre': str,
                                           'VerhaeltnisReferenzertragErtrag10Jahre': str,
                                           'VerhaeltnisReferenzertragErtrag15Jahre': str,
                                           'AusschreibungZuschlag': np.bool,
                                           'Zuschlagsnummer': str,
                                           'AnlageBetriebsstatus': str,
                                           'VerknuepfteEinheit': str,
                                           'version': str,
                                           'timestamp': str})
        # log.info(f'Read data from {csv_name}')
        print(f'Read data from {csv_name}.')
    return unit_wind_eeg


def write_to_csv(csv_name, df):
    """Create CSV file if not exists and append data.

    Parameters
    ----------
    csv_name : str
        Name of file.
    df : DataFrame
        data saved to file
    """
    if not os.path.isfile(csv_name):
        with codecs.open(csv_name, mode='w', encoding='utf-8', errors='replace') as file:
            df.to_csv(file, sep=';', header=True, line_terminator='\n')
            # log.info(f'Created {csv_name} with header.')
            print(f'Created {csv_name} with header.')
    else:
        with open(csv_name, mode='a') as file:
            df.to_csv(file, sep=';', header=False, line_terminator='\n')
            # log.info(f'Inserted data into {csv_name}')
            # print(f'Inserted data into {csv_name}.')


def setup_power_unit_wind():
    """Setup file for Stromerzeugungseinheit-Wind.

    Check if file with Stromerzeugungseinheit-Wind exists. Create if not exists.
    Load Stromerzeugungseinheit-Wind from file if exists.

    Returns
    -------
    power_unit_wind : DataFrame
        Stromerzeugungseinheit-Wind.
    """
    data_version = get_data_version()
    csv_see = f'data/bnetza_mastr_{data_version}_stromerzeuger.csv'
    csv_see_wind = f'data/bnetza_mastr_{data_version}_stromerzeuger_wind.csv'
    if not os.path.isfile(csv_see_wind):
        power_unit = enc_read_power_units(csv_see)
        power_unit = power_unit.drop_duplicates()
        power_unit_wind = power_unit[power_unit.Einheittyp == 'Windeinheit']
        power_unit_wind.index.names = ['see_id']
        power_unit_wind.reset_index()
        power_unit_wind.index.names = ['id']
        # log.info(f'Filtered Wind from Stromerzeuger')
        print(f'Filtered Wind from Stromerzeuger.')
        try:
            write_to_csv(csv_see_wind, power_unit_wind)
            return power_unit_wind
        except:
            # log.info(f'Not able to write to file: {csv_see_wind}')
            print(f'Not able to write to file: {csv_see_wind}.')
    else:
        power_unit_wind = enc_read_power_units(csv_see_wind)
        # log.info(f'Read Stromerzeugungseinheit-Wind from {csv_see_wind}.')
        print(f'Read Stromerzeugungseinheit-Wind from {csv_see_wind}.')
        return power_unit_wind


def download_power_unit():
    """Download StromErzeuger.

    Existing units: 1822000 (2019-02-10)
    """
    power_unit_list_len = 1822000

    data_version = get_data_version()
    csv_see = f'data/bnetza_mastr_{data_version}_stromerzeuger.csv'
    # log.info(f'Number of expected StromErzeuger: {power_unit_list_len}')
    print(f'Number of expected StromErzeuger: {power_unit_list_len}.')

    for start_from in range(0, power_unit_list_len, 2000):
        try:
            power_unit = get_power_unit(start_from)
            write_to_csv(csv_see, power_unit)

            power_unit_len = len(power_unit)
            # log.info(f'Downloaded StromErzeuger from {start_from}-{start_from + power_unit_len}')
            print(f'Downloaded StromErzeuger from {start_from}-{start_from + power_unit_len}.')
        except:
            # log.info(f'Not able to download StromErzeuger from {start_from}')
            print(f'Not able to download StromErzeuger from {start_from}.')


def download_unit_wind():
    """Download Windeinheit.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0

    data_version = get_data_version()
    csv_wind = f'data/bnetza_mastr_{data_version}_windeinheit.csv'
    unit_wind = setup_power_unit_wind()
    unit_wind_list = unit_wind['EinheitMastrNummer'].values.tolist()
    unit_wind_list_len = len(unit_wind_list)
    # log.info(f'Number of Windeinheit: {WindMastrNrListLen}')
    print(f'Number of Windeinheit: {unit_wind_list_len}.')

    for i in range(start_from, unit_wind_list_len, 1):
        try:
            unit_wind = get_power_unit_wind(unit_wind_list[i])
            write_to_csv(csv_wind, unit_wind)
            # log.info(f'Downloaded Windeinheit ({i}): {unit_wind_list[i]}')
            print(f'Downloaded Windeinheit ({i}): {unit_wind_list[i]}.')
            # time.sleep(1)
        except:
            # log.info(f'Not able to download Windeinheit ({i}): {unit_wind_list[i]}')
            print(f'Not able to download Windeinheit ({i}): {unit_wind_list[i]}.')


def download_unit_wind_eeg():
    """Download unit_wind_eeg using GetAnlageEegWind request."""
    data_version = get_data_version()
    csv_wind_eeg = f'data/bnetza_mastr_{data_version}_windeeg.csv'
    unit_wind = setup_power_unit_wind()

    unit_wind_list = unit_wind['EegMastrNummer'].values.tolist()
    unit_wind_list_len = len(unit_wind_list)
    # log.info(f'Count of EEG-Anlage-Wind: {unit_wind_list_len}')
    print(f'Count of EEG-Anlage-Wind: {unit_wind_list_len}.')

    for i in range(0, unit_wind_list_len, 1):
        try:
            unit_wind_eeg = get_unit_wind_eeg(unit_wind_list[i])
            write_to_csv(csv_wind_eeg, unit_wind_eeg)
            # log.info(f'Download EEG-Anlage-Wind ({i}): {unit_wind_list[i]}')
            print(f'Download EEG-Anlage-Wind ({i}): {unit_wind_list[i]}.')
        except:
            # log.info(f'Failed download EEG-Anlage-Wind ({i}): {unit_wind_list[i]}')
            print(f'Failed download EEG-Anlage-Wind ({i}): {unit_wind_list[i]}.')
