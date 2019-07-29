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
__version__ = "v0.7.0"

from utils import get_data_version, write_to_csv, get_filename_csv_see, set_filename_csv_see, get_correct_filepath
from sessions import mastr_session
from mastr_power_unit_download import read_power_units

import pandas as pd
import numpy as np
import datetime
import os
from zeep.helpers import serialize_object

import logging
log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


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
    c = disentangle_manufacturer(c)
    del c['Hersteller']
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_wind = df.set_index(list(df.columns.values)[0]).transpose()
    unit_wind.reset_index()
    unit_wind.index.names = ['lid']
    unit_wind['version'] = data_version
    unit_wind['timestamp'] = str(datetime.datetime.now())
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
                                       'Ergebniscode': str,
                                       'AufrufVeraltet': str,
                                       'AufrufLebenszeitEnde': str,
                                       'AufrufVersion': str,
                                       'GenMastrNummer': str,
                                       'Art': str,
                                       'Datum': str,
                                       'Behoerde': str,
                                       'Aktenzeichen': str,
                                       'Frist': str,
                                       'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_wind_permit


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
    #csv_see = get_correct_filepath()
    #set_corrected_path(csv_see)
    from utils import csv_see_wind, csv_see
    if not os.path.isfile(csv_see_wind):
        power_unit = read_power_units(csv_see)
        power_unit = power_unit.drop_duplicates()
        power_unit_wind = power_unit[power_unit.Einheittyp == 'Windeinheit']
        power_unit_wind.index.names = ['see_id']
        power_unit_wind.reset_index()
        power_unit_wind.index.names = ['id']
        # log.info(f'Write data to {csv_see_wind}')
        write_to_csv(csv_see_wind, power_unit_wind)
        return power_unit_wind
    else:
        power_unit_wind = read_power_units(csv_see_wind)
        # log.info(f'Read data from {csv_see_wind}')
        return power_unit_wind


def download_unit_wind():
    """Download Windeinheit.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0

    set_filename_csv_see('wind_units', overwrite=True)
    from utils import csv_see_wind as csv_see
    unit_wind_list = unit_wind['EinheitMastrNummer'].values.tolist()
    unit_wind_list_len = len(unit_wind_list)
    log.info('Download MaStR Wind')
    log.info(f'Number of unit_wind: {unit_wind_list_len}')

    for i in range(start_from, unit_wind_list_len, 1):
        try:
            unit_wind = get_power_unit_wind(unit_wind_list[i])
            write_to_csv(csv_wind, unit_wind)
        except:
            log.exception(f'Download failed unit_wind ({i}): {unit_wind_list[i]}')

def download_unit_wind_eeg():
    """Download unit_wind_eeg using GetAnlageEegWind request."""
    data_version = get_data_version()
    csv_wind_eeg = f'data/bnetza_mastr_{data_version}_unit-wind-eeg.csv'
    unit_wind = setup_power_unit_wind()

    unit_wind_list = unit_wind['EegMastrNummer'].values.tolist()
    unit_wind_list_len = len(unit_wind_list)

    for i in range(0, unit_wind_list_len, 1):
        try:
            unit_wind_eeg = get_unit_wind_eeg(unit_wind_list[i])
            write_to_csv(csv_wind_eeg, unit_wind_eeg)
        except:
            log.exception(f'Download failed unit_wind_eeg ({i}): {unit_wind_list[i]}')

def download_unit_wind_permit():
        """Download unit_wind_permit using GetEinheitGenehmigung request."""
        data_version = get_data_version()
        csv_wind_permit = f'data/bnetza_mastr_{data_version}_unit-wind-permit.csv'
        unit_wind = setup_power_unit_wind()

        unit_wind_list = unit_wind['GenMastrNummer'].values.tolist()
        unit_wind_list_len = len(unit_wind_list)

        for i in range(0, unit_wind_list_len, 1):
            try:
                unit_wind_permit = get_unit_wind_permit(unit_wind_list[i])
                write_to_csv(csv_wind_permit, unit_wind_permit)
            except:
                log.exception(f'Download failed unit_wind_permit ({i}): {unit_wind_list[i]}')


def disentangle_manufacturer(wind_unit):
    wu = wind_unit
    try:
        wu['HerstellerID'] = wind_unit['Hersteller']['Id']
        wu['HerstellerName'] = wind_unit['Hersteller']['Wert']
        return(wu)
    except:
        print("This wind_unit contains no OrderedDict for 'Hersteller'")
        return(wind_unit)
