#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Nuclear

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

from open_mastr.soap_api.sessions import mastr_session
from open_mastr.soap_api.utils import write_to_csv, get_data_version, read_power_units
from open_mastr.soap_api.utils import (fname_power_unit,
                            fname_power_unit_nuclear,
                            fname_nuclear_unit)

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


# Setup power-unit
def setup_power_unit_nuclear():
    """Setup file for Stromerzeugungseinheit-Kernenergie (power-unit_nuclear).

    Check if file with Stromerzeugungseinheit (power-unit) exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Kernenergie.
    Remove duplicates and write to file.

    Returns
    -------
    fname_power_unit_nuclear : csv
        Write Stromerzeugungseinheit-Kernenergie to csv file.
    """
    if os.path.isfile(fname_power_unit_nuclear):
        log.info(f'Skip setup for Stromerzeugungseinheit-Kernenergie')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

            power_unit_nuclear = power_unit[power_unit.Einheittyp == 'Kernenergie']
            power_unit_nuclear = power_unit_nuclear.drop_duplicates(subset=['EinheitMastrNummer',
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
            log.info(f'Filter power-unit for Kernenergie and remove duplicates')
            power_unit_nuclear.reset_index()
            power_unit_nuclear.index.name = 'pu-id'

            write_to_csv(fname_power_unit_nuclear, power_unit_nuclear)
            power_unit_nuclear_cnt = power_unit_nuclear['timestamp'].count()
            log.info(f'Write {power_unit_nuclear_cnt} power-unit_nuclear to {fname_power_unit_nuclear}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_nuclear(csv_name):
    """Read Stromerzeugungseinheit-Kernenergie (power-unit_nuclear) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_nuclear : DataFrame
        Stromerzeugungseinheit-Kernenergie.
    """
    if os.path.isfile(csv_name):
        power_unit_nuclear = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                                         dtype={
                                             'pu-id': str,
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
        power_unit_nuclear_cnt = power_unit_nuclear['timestamp'].count()
        log.info(f'Read {power_unit_nuclear_cnt} Stromerzeugungseinheit-Kernenergie from {csv_name}')
        return power_unit_nuclear

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-nuclear
def download_unit_nuclear():
    """Download Kernenergieeinheit. Write results to csv file.


    ofname : string
        Path to save the downloaded files.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0
    setup_power_unit_nuclear()

    power_unit_nuclear = read_power_unit_nuclear(fname_power_unit_nuclear)
    power_unit_nuclear = power_unit_nuclear['EinheitMastrNummer']
    mastr_list = power_unit_nuclear.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Kernenergieeinheit')

    for i in range(start_from, mastr_list_len, 1):
        unit_nuclear = get_power_unit_nuclear(mastr_list[i])  # First download
        if unit_nuclear is not None:
            write_to_csv(fname_nuclear_unit, unit_nuclear)
        else:
            log.exception(f'First download failed unit_nuclear ({i}): {mastr_list[i]}', exc_info=False)
            unit_nuclear = get_power_unit_nuclear(mastr_list[i])  # Second download
            if unit_nuclear is not None:
                write_to_csv(fname_nuclear_unit, unit_nuclear)
            else:
                log.exception(f'Second download failed unit_nuclear ({i}): {mastr_list[i]}', exc_info=False)


def get_power_unit_nuclear(mastr_unit_nuclear):
    """Get Kernenergieeinheit from API using GetEinheitKernenergie.

    Parameters
    ----------
    mastr_unit_nuclear : object
        Kernenergie from EinheitMastrNummerId.

    Returns
    -------
    unit_nuclear : DataFrame
        Kernenergieeinheit.
    """

    data_version = get_data_version()
    try:
        c = client_bind.GetEinheitKernkraft(apiKey=api_key,
                                            marktakteurMastrNummer=my_mastr,
                                            einheitMastrNummer=mastr_unit_nuclear)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_nuclear = df.set_index(list(df.columns.values)[0]).transpose()
        unit_nuclear.reset_index()
        unit_nuclear.index.names = ['lid']
        unit_nuclear['version'] = data_version
        unit_nuclear['timestamp'] = str(datetime.datetime.now())
        return unit_nuclear
    except Exception as e:
        # log.info('Download failed for %s', mastr_unit_nuclear)
        pass


def read_unit_nuclear(csv_name):
    """Read Kernenergieeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_nuclear : DataFrame
        Kernenergieeinheit.
    """
    if os.path.isfile(csv_name):
        unit_nuclear = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
                                          'AltAnlagenbetreiberMastrNummer': str,
                                          'DatumDesBetreiberwechsels': str,
                                          'DatumRegistrierungDesBetreiberwechsels': str,
                                          'StatisikFlag': str,
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
                                          'NameKraftwerk': str,
                                          'NameKraftwerksblock': str,
                                          'Technologie': str,
                                          'version': str,
                                          'timestamp': str})
        unit_nuclear_cnt = unit_nuclear['timestamp'].count()
        log.info(f'Read {unit_nuclear_cnt} Kernenergieeinheiten from {csv_name}')
        return unit_nuclear

    else:
        log.info(f'Error reading {csv_name}')
