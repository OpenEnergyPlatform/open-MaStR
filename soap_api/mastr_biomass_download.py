#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Biomass

Read data from MaStR API and write to CSV files.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli; Bachibouzouk; solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.8.0"


import os
import datetime
import logging
import pandas as pd

from zeep.helpers import serialize_object

from soap_api.utils import get_data_version, write_to_csv
from soap_api.sessions import mastr_session
from soap_api.mastr_power_unit_download import read_power_units
""" import variables """
from soap_api.utils import (
    fname_all_units,
    fname_power_unit_biomass,
    fname_unit_biomass,
    fname_unit_biomass_eeg
)

log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def get_power_unit_biomass(mastr_unit_biomass):
    """Get Biomasseeinheit from API using GetEinheitBiomasse.

    Parameters
    ----------
    mastr_unit_biomass : object
        Biomass from EinheitMastrNummerId.

    Returns
    -------
    unit_biomass : DataFrame
        Biomasseeinheit.
    """
    data_version = get_data_version()
    c = client_bind.GetEinheitBiomasse(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        einheitMastrNummer=mastr_unit_biomass
    )
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_biomass = df.set_index(list(df.columns.values)[0]).transpose()
    unit_biomass.reset_index()
    unit_biomass.index.names = ['lid']
    unit_biomass['version'] = data_version
    unit_biomass['timestamp'] = str(datetime.datetime.now())
    return unit_biomass


def read_unit_biomass(csv_name):
    """Read Biomasseeinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_biomass : DataFrame
        Biomasseeinheit.
    """
    # log.info(f'Read data from {csv_name}')
    unit_biomass = pd.read_csv(
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
            'Hauptbrennstoff': str,
            'Biomasseart': str,
            'Technologie': str,
            'EegMastrNummer': str,
            'KwkMastrNummer': str,
            'version': str,
            'timestamp': str
        }
    )
    # log.info(f'Finished reading data from {csv_name}')
    return unit_biomass


def get_unit_biomass_eeg(mastr_biomass_eeg):
    """Get EEG-Anlage-Biomasse from API using GetAnlageEegBiomasse.

    Parameters
    ----------
    mastr_biomass_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_biomass_eeg : DataFrame
        EEG-Anlage-Biomasse.
    """
    data_version = get_data_version()
    c = client_bind.GetAnlageEegBiomasse(
        apiKey=api_key,
        marktakteurMastrNummer=my_mastr,
        eegMastrNummer=mastr_biomass_eeg
    )
    s = serialize_object(c)
    df = pd.DataFrame(list(s.items()), )
    unit_biomass_eeg = df.set_index(list(df.columns.values)[0]).transpose()
    unit_biomass_eeg.reset_index()
    unit_biomass_eeg.index.names = ['lid']
    unit_biomass_eeg["version"] = data_version
    unit_biomass_eeg["timestamp"] = str(datetime.datetime.now())
    return unit_biomass_eeg


def read_unit_biomass_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Biomass from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_biomass_eeg : DataFrame
        EEG-Anlage-Biomass
    """
    # log.info(f'Read data from {csv_name}')
    unit_biomass_eeg = pd.read_csv(
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
            'AusschliesslicheVerwendungBiomasse': str,
            'AusschreibungZuschlag': str,
            'Zuschlagsnummer': str,
            'BiogasInanspruchnahmeFlexiPraemie': str,
            'BiogasDatumInanspruchnahmeFlexiPraemie': str,
            'BiogasLeistungserhoehung': str,
            'BiogasDatumLeistungserhoehung': str,
            'BiogasUmfangLeistungserhoehung': str,
            'BiogasGaserzeugungskapazitaet': str,
            'BiogasHoechstbemessungsleistung': str,
            'BiomethanErstmaligerEinsatz': str,
            'AnlageBetriebsstatus': str,
            'VerknuepfteEinheit': str,
            'version': str,
            'timestamp': str
        }
    )
    # log.info(f'Finished reading data from {csv_name}')
    return unit_biomass_eeg


def setup_power_unit_biomass(ofname=None):
    """Setup file for Stromerzeugungseinheit-Biomass.

    Check if file with Stromerzeugungseinheit-Biomass exists. Create if not exists.
    Load Stromerzeugungseinheit-Biomass from file if exists.

    ofname : string
        Path to save the downloaded files.

    Returns
    -------
    power_unit_biomass : DataFrame
        Stromerzeugungseinheit-Biomass.
    """
    # assign file name default value
    if ofname is None:
        ofname = fname_power_unit_biomass

    if os.path.isfile(fname_all_units):
        power_unit = read_power_units(fname_all_units)
        if not power_unit.empty:
            power_unit = power_unit.drop_duplicates()
            power_unit_biomass = power_unit[power_unit.Einheittyp == 'Biomasse']
            power_unit_biomass.index.names = ['see_id']
            power_unit_biomass.reset_index()
            power_unit_biomass.index.names = ['id']
            write_to_csv(ofname, power_unit_biomass)
            power_unit.iloc[0:0]
            return power_unit_biomass
    else:
        log.info('no biomassunits found')
        return pd.DataFrame()


def download_unit_biomass(overwrite=False, ofname=None):
    """Download Biomasseeinheit.


    ofname : string
        Path to save the downloaded files.

    Existing units: 31543 (2019-02-10)
    """
    # assign file name default value
    if ofname is None:
        ofname = fname_unit_biomass

    start_from = 0
    unit_biomass = setup_power_unit_biomass(overwrite)
    unit_biomass_list = unit_biomass['EinheitMastrNummer'].values.tolist()
    unit_biomass_list_len = len(unit_biomass_list)
    log.info('Download MaStR Biomass')
    log.info(f'Number of unit_biomass: {unit_biomass_list_len}')

    for i in range(start_from, unit_biomass_list_len, 1):
        try:
            unit_biomass = get_power_unit_biomass(unit_biomass_list[i])
            write_to_csv(ofname, unit_biomass)
        except:
            log.exception(f'Download failed unit_biomass ({i}): {unit_biomass_list[i]}')


def download_unit_biomass_eeg(ofname=None):
    """Download unit_biomass_eeg using GetAnlageEegBiomasse request.

    ofname : string
        Path to save the downloaded files.
    """
    # assign file name default value
    if ofname is None:
        ofname = fname_unit_biomass_eeg

    unit_biomass = setup_power_unit_biomass()

    unit_biomass_list = unit_biomass['EegMastrNummer'].values.tolist()
    unit_biomass_list_len = len(unit_biomass_list)

    for i in range(0, unit_biomass_list_len, 1):
        try:
            unit_biomass_eeg = get_unit_biomass_eeg(unit_biomass_list[i])
            write_to_csv(ofname, unit_biomass_eeg)
        except:
            log.exception(f'Download failed unit_biomass_eeg ({i}): {unit_biomass_list[i]}')
