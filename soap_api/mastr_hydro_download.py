#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Hydro

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

import os
import datetime
import logging
import pandas as pd

from zeep.helpers import serialize_object

from soap_api.utils import get_data_version, write_to_csv, read_power_units
from soap_api.sessions import mastr_session
""" import variables """
from soap_api.utils import (
    fname_power_unit,
    fname_power_unit_hydro,
    fname_hydro_unit,
    fname_hydro_unit_eeg
)

log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit_hydro(mastr_unit_hydro):
    """Get Wassereinheit from API using GetEinheitWasser.

    Parameters
    ----------
    mastr_unit_hydro : object
        Wasser from EinheitMastrNummerId.

    Returns
    -------
    unit_hydro : DataFrame
        Wassereinheit.
    """
    data_version = get_data_version()
    c = client_bind.GetEinheitWasser(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        einheitMastrNummer=mastr_unit_hydro
    )
    # c = disentangle_manufacturer(c)
    # del c['Hersteller']
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_hydro = df.set_index(list(df.columns.values)[0]).transpose()
    unit_hydro.reset_index()
    unit_hydro.index.names = ['lid']
    unit_hydro['version'] = data_version
    unit_hydro['timestamp'] = str(datetime.datetime.now())
    return unit_hydro


def read_unit_hydro(csv_name):
    """Read Wassereinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_hydro : DataFrame
        Wassereinheit.
    """
    # log.info(f'Read data from {csv_name}')
    unit_hydro = pd.read_csv(
        csv_name,
        header=0,
        encoding='utf-8',
        sep=';',
        index_col=False,
        dtype={
            'lid': int,
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
            'NameKraftwerk': str,
            'ArtDerWasserkraftanlage': str,
            'AnzeigeEinerStilllegung': str,
            'ArtDerStilllegung': str,
            'DatumBeginnVorlaeufigenOderEndgueltigenStilllegung': str,
            'MinderungStromerzeugung': str,
            'BestandteilGrenzkraftwerk': str,
            'NettonennleistungDeutschland': str,
            'ArtDesZuflusses': str,
            'EegMastrNummer': str,
            'version': str,
            'timestamp': str
        }
    )
    # log.info(f'Finished reading data from {csv_name}')
    return unit_hydro


def get_unit_hydro_eeg(mastr_hydro_eeg):
    """Get EEG-Anlage-Wasser from API using GetAnlageEegWasser.

    Parameters
    ----------
    mastr_hydro_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_hydro_eeg : DataFrame
        EEG-Anlage-Wasser.
    """
    data_version = get_data_version()
    c = client_bind.GetAnlageEegWasser(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        eegMastrNummer=mastr_hydro_eeg
    )
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_hydro_eeg = df.set_index(list(df.columns.values)[0]).transpose()
    unit_hydro_eeg.reset_index()
    unit_hydro_eeg.index.names = ['lid']
    unit_hydro_eeg["version"] = data_version
    unit_hydro_eeg["timestamp"] = str(datetime.datetime.now())
    return unit_hydro_eeg


def read_unit_hydro_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Wasser from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_hydro_eeg : DataFrame
        EEG-Anlage-Wasser
    """
    # log.info(f'Read data from {csv_name}')
    unit_hydro_eeg = pd.read_csv(
        csv_name,
        header=0,
        sep=';',
        index_col=False,
        encoding='utf-8',
        dtype={
            'lid': int,
            'Ergebniscode': str,
            'AufrufVeraltet': str,
            'AufrufLebenszeitEnde': str,
            'AufrufVersion': str,
            'Meldedatum': str,
            'DatumLetzteAktualisierung': str,
            'EegInbetriebnahmedatum': str,
            'EegMastrNummer': str,
            'AnlagenschluesselEeg': str,
            'AnlagenkennzifferAnlagenregister': str,
            'InstallierteLeistung': str,
            'AnlageBetriebsstatus': str,
            'Ertuechtigung': str,
            'VerknuepfteEinheit': str,
            'version': str,
            'timestamp': str
        }
    )
    # log.info(f'Finished reading data from {csv_name}')
    return unit_hydro_eeg


def setup_power_unit_hydro(ofname=None):
    """Setup file for Stromerzeugungseinheit-Wasser.

    Check if file with Stromerzeugungseinheit-Wasser exists. Create if not exists.
    Load Stromerzeugungseinheit-Wasser from file if exists.

    ofname : string
        Path to save the downloaded files.

    Returns
    -------
    power_unit_hydro : DataFrame
        Stromerzeugungseinheit-Wasser.
    """
    # assign file name default value
    if ofname is None:
        ofname = fname_power_unit_hydro

    if os.path.isfile(fname_power_unit):
        power_unit = read_power_units(fname_power_unit)
        power_unit = power_unit.drop_duplicates()
        power_unit_hydro = power_unit[power_unit.Einheittyp == 'Wasser']
        power_unit_hydro.index.names = ['see_id']
        power_unit_hydro.reset_index()
        power_unit_hydro.index.names = ['id']
        write_to_csv(ofname, power_unit_hydro)
        power_unit_hydro.iloc[0:0]
        return power_unit_hydro
    else:
        log.info('no hydrounits found')
        return pd.DataFrame()


def download_unit_hydro(ofname=None):
    """Download Hydroeinheit.

    ofname : string
        Path to save the downloaded files.
    """
    # assign file name default value
    if ofname is None:
        ofname = fname_hydro_unit

    start_from = 0
    unit_hydro = setup_power_unit_hydro()
    unit_hydro_list = unit_hydro['EinheitMastrNummer'].values.tolist()
    unit_hydro_list_len = len(unit_hydro_list)
    log.info('Download MaStR Hydro')
    log.info(f'Number of unit_hydro: {unit_hydro_list_len}')

    for i in range(start_from, unit_hydro_list_len, 1):
        try:
            unit_hydro = get_power_unit_hydro(unit_hydro_list[i])
            write_to_csv(ofname, unit_hydro)
        except:
            log.exception(f'Download failed unit_hydro ({i}): {unit_hydro_list[i]}')


def download_unit_hydro_eeg(ofname=None):
    """Download unit_hydro_eeg using GetAnlageEegWasser request.

    ofname : string
        Path to save the downloaded files.
    """
    # assign file name default value
    if ofname is None:
        ofname = fname_hydro_unit_eeg

    unit_hydro = setup_power_unit_hydro()

    unit_hydro_list = unit_hydro['EegMastrNummer'].values.tolist()
    unit_hydro_list_len = len(unit_hydro_list)

    for i in range(0, unit_hydro_list_len, 1):
        try:
            unit_hydro_eeg = get_unit_hydro_eeg(unit_hydro_list[i])
            write_to_csv(ofname, unit_hydro_eeg)
        except:
            log.exception(f'Download failed unit_hydro_eeg ({i}): {unit_hydro_list[i]}')
