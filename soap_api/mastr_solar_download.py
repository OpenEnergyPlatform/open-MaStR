#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BNetzA - MaStR Download - Solar

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
from soap_api.utils import get_data_version, write_to_csv, remove_csv, read_power_units
from soap_api.utils import (fname_power_unit,
                            fname_power_unit_solar,
                            fname_solar_unit,
                            fname_solar_eeg)
from soap_api.parallel import parallel_download

import pandas as pd
import datetime
import os
from zeep.helpers import serialize_object
import textwrap

import logging

log = logging.getLogger(__name__)

"""SOAP API"""
client, client_bind, token, user = mastr_session()
api_key = token
my_mastr = user


def setup_power_unit_solar():
    """Setup file for Stromerzeugungseinheit-Solar (power-unit_solar).

    Check if file with Stromerzeugungseinheit (power-unit) exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Solar.
    Remove duplicates and write to file.

    Returns
    -------
    fname_power_unit_solar : csv
        Write Stromerzeugungseinheit-Solar to csv file.
    """
    if os.path.isfile(fname_power_unit_solar):
        log.info(f'Skip setup for Stromerzeugungseinheit-Solar')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

            power_unit_solar = power_unit[power_unit.Einheittyp == 'Solareinheit']
            power_unit_solar = power_unit_solar.drop_duplicates(subset=['EinheitMastrNummer',
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
            log.info(f'Filter power-unit for solar and remove duplicates')
            power_unit_solar.reset_index()
            power_unit_solar.index.name = 'pu-id'

            write_to_csv(fname_power_unit_solar, power_unit_solar)
            power_unit_solar_cnt = power_unit_solar['timestamp'].count()
            log.info(f'Write {power_unit_solar_cnt} power-unit_solar to {fname_power_unit_solar}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_solar(csv_name):
    """Encode and read Stromerzeugungseinheit-Solar (power-unit_solar) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    power_unit_solar : DataFrame
        Stromerzeugungseinheit-Solar.
    """
    if os.path.isfile(csv_name):
        power_unit_solar = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
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
        power_unit_solar_cnt = power_unit_solar['timestamp'].count()
        log.info(f'Read {power_unit_solar_cnt} Stromerzeugungseinheit-Solar from {csv_name}')
        return power_unit_solar

    else:
        log.info(f'Error reading {csv_name}')


def download_parallel_unit_solar(unit_list, threads=4, timeout=10, time_blacklist=True):
    return parallel_download(
        unit_list,
        get_power_unit_solar,
        fname_solar_unit,
        threads=threads,
        timeout=timeout,
        time_blacklist=time_blacklist)

def get_power_unit_solar(mastr_unit_solar):
    """Get Solareinheit from API using GetEinheitSolar.

    Parameters
    ----------
    mastr_unit_solar : object
        Solar from EinheitMastrNummerId.

    Returns
    -------
    unit_solar : DataFrame
        Solareinheit.
    """
    # with mp.Lock():
    #   log.info('downloading data unit... %s', mastr_unit_solar)
    data_version = get_data_version()
    unit_solar = pd.DataFrame()
    try:
        c = client_bind.GetEinheitSolar(apiKey=api_key,
                                        marktakteurMastrNummer=my_mastr,
                                        einheitMastrNummer=mastr_unit_solar)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_solar = df.set_index(list(df.columns.values)[0]).transpose()
        unit_solar.reset_index()
        unit_solar.index.names = ['lid']
        unit_solar['version'] = data_version
        unit_solar['timestamp'] = str(datetime.datetime.now())
        return unit_solar
    except Exception as e:
        log.info('Download failed for {}: {}'.format(mastr_unit_solar, e))


def read_unit_solar(csv_name):
    """Read Solareinheit from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_solar : DataFrame
        Solareinheit.
    """
    # log.info(f'Read data from {csv_name}')
    unit_solar = pd.read_csv(csv_name, header=0, encoding='utf-8', sep=';', index_col=False,
                             dtype={'lid': str,
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
                                    'Bruttoleistung': str,
                                    'Nettonennleistung': str,
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
                                    'zugeordneteWirkleistungWechselrichter': str,
                                    'GemeinsamerWechselrichterMitSpeicher': str,
                                    'AnzahlModule': str,
                                    'Lage': str,
                                    'Leistungsbegrenzung': str,
                                    'EinheitlicheAusrichtungUndNeigungswinkel': str,
                                    'Hauptausrichtung': str,
                                    'HauptausrichtungNeigungswinkel': str,
                                    'Nebenausrichtung': str,
                                    'NebenausrichtungNeigungswinkel': str,
                                    'InAnspruchGenommeneFlaeche': str,
                                    'ArtDerFlaeche': str,
                                    'InAnspruchGenommeneAckerflaeche': str,
                                    'Nutzungsbereich': str,
                                    'EegMastrNummer': str,
                                    'version': str,
                                    'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_solar


def download_parallel_unit_solar_eeg(unit_list, threads=4, timeout=10, time_blacklist=True):
    return parallel_download(
        unit_list,
        get_unit_solar_eeg,
        fname_solar_eeg,
        threads=threads,
        timeout=timeout,
        time_blacklist=time_blacklist)

def get_unit_solar_eeg(mastr_solar_eeg):
    """Get EEG-Anlage-Solar from API using GetAnlageEegSolar.

    Parameters
    ----------
    mastr_solar_eeg : str
        MaStR EEG Nr.

    Returns
    -------
    unit_solar_eeg : DataFrame
        EEG-Anlage-Solar.
    """
    data_version = get_data_version()
    try:
        c = client_bind.GetAnlageEegSolar(apiKey=api_key,
                                          marktakteurMastrNummer=my_mastr,
                                          eegMastrNummer=mastr_solar_eeg)
        s = serialize_object(c)
        df = pd.DataFrame(list(s.items()), )
        unit_solar_eeg = df.set_index(list(df.columns.values)[0]).transpose()
        unit_solar_eeg.reset_index()
        unit_solar_eeg.index.names = ['lid']
        unit_solar_eeg["version"] = data_version
        unit_solar_eeg["timestamp"] = str(datetime.datetime.now())
    except Exception as e:
        return None
    return unit_solar_eeg


def read_unit_solar_eeg(csv_name):
    """
    Encode and read EEG-Anlage-Solar from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_solar_eeg : DataFrame
        EEG-Anlage-Solar
    """
    # log.info(f'Read data from {csv_name}')
    unit_solar_eeg = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                 dtype={'lid': int,
                                        'Ergebniscode': str,
                                        'AufrufVeraltet': str,
                                        'AufrufLebenszeitEnde': str,
                                        'AufrufVersion': str,
                                        'Meldedatum': str,
                                        'DatumLetzteAktualisierung': str,
                                        'EegInbetriebnahmedatum': str,
                                        'EegMastrNummer': str,
                                        'InanspruchnahmeZahlungNachEeg': str,
                                        'AnlagenschluesselEeg': str,
                                        'AnlagenkennzifferAnlagenregister': str,
                                        'InstallierteLeistung': str,
                                        'RegistrierungsnummerPvMeldeportal': str,
                                        'MieterstromZugeordnet': str,
                                        'MieterstromMeldedatum': str,
                                        'MieterstromErsteZuordnungZuschlag': str,
                                        'AusschreibungZuschlag': str,
                                        'ZugeordneteGebotsmenge': str,
                                        'Zuschlagsnummer': str,
                                        'AnlageBetriebsstatus': str,
                                        'VerknuepfteEinheit': str,
                                        'version': str,
                                        'timestamp': str})
    # log.info(f'Finished reading data from {csv_name}')
    return unit_solar_eeg

# Download unit-solar
def download_unit_solar():
    """Download Solareinheit.

    Existing units: 31543 (2019-02-10)
    """
    start_from = 0
    log.info('download unit solar..')
    unit_solar = setup_power_unit_solar(eeg=False)
    unit_solar_list = unit_solar['EinheitMastrNummer'].values.tolist()
    unit_solar_list_len = len(unit_solar_list)
    log.info(f'Download MaStR Solar')
    log.info(f'Number of unit_solar: {unit_solar_list_len}')

    for i in range(start_from, unit_solar_list_len, 1):
        try:
            unit_solar = get_power_unit_solar(unit_solar_list[i])
            write_to_csv(fname_solar_unit, unit_solar)
        except:
            log.exception(f'Download failed unit_solar ({i}): {unit_solar_list[i]}')

# Download unit-solar-eeg
def download_unit_solar_eeg():
    """Download unit_solar_eeg using GetAnlageEegSolar request.

    Parameters
    ----------
    sublist : list
        list to process in parallel
    parallelism : int
        number of threads
    """
    data_version = get_data_version()
    unit_solar = setup_power_unit_solar()

    unit_solar_list = unit_solar['EegMastrNummer'].values.tolist()
    unit_solar_list_len = len(unit_solar_list)

    for i in range(0, unit_solar_list_len, 1):
        try:
            unit_solar_eeg = get_unit_solar_eeg(unit_solar_list[i])
            write_to_csv(fname_solar_eeg, unit_solar_eeg)
        except:
            log.exception(f'Download failed unit_solar_eeg ({i}): {unit_solar_list[i]}')
