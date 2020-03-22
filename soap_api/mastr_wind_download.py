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
from soap_api.utils import (fname_power_unit,
                            fname_power_unit_wind,
                            fname_wind_unit,
                            fname_wind_eeg,
                            fname_wind_permit,
                            fname_wind_fail_u,
                            fname_wind_fail_e,
                            fname_wind_fail_p)

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
def setup_power_unit_wind():
    """Setup file for Stromerzeugungseinheit-Wind (power-unit_wind).

    Check if file with Stromerzeugungseinheit (power-unit) exists.
    Read Stromerzeugungseinheit and filter Stromerzeugungseinheit-Wind.
    Remove duplicates and write to file.

    Returns
    -------
    fname_power_unit_wind : csv
        Write Stromerzeugungseinheit-Wind to csv file.
    """
    if os.path.isfile(fname_power_unit_wind):
        log.info(f'Skip setup for Stromerzeugungseinheit-Wind')

    else:
        if os.path.isfile(fname_power_unit):
            power_unit = read_power_units(fname_power_unit)

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
            power_unit_wind.reset_index()
            power_unit_wind.index.name = 'pu-id'

            write_to_csv(fname_power_unit_wind, power_unit_wind)
            power_unit_wind_cnt = power_unit_wind['timestamp'].count()
            log.info(f'Write {power_unit_wind_cnt} power-unit_wind to {fname_power_unit_wind}')

        else:
            log.info(f'Error reading power-unit from {fname_power_unit}')


def read_power_unit_wind(csv_name):
    """Encode and read Stromerzeugungseinheit-Wind (power-unit_wind) from CSV file.

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
        power_unit_wind_cnt = power_unit_wind['timestamp'].count()
        log.info(f'Read {power_unit_wind_cnt} Stromerzeugungseinheit-Wind from {csv_name}')
        return power_unit_wind

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-wind
def download_unit_wind():
    """Download Windeinheit (unit-wind).

    Filter EinheitMastrNummer from Stromerzeugungseinheit-Wind.
    Remove duplicates and count.
    Loop over list and write download to file.

    Existing units: 31543 (2019-02-10)

    Returns
    -------
    fname_wind_unit : csv
        Write Windeinheit to csv file.
    """
    start_from = 0
    setup_power_unit_wind()
    power_unit_wind = read_power_unit_wind(fname_power_unit_wind)
    power_unit_wind = power_unit_wind['EinheitMastrNummer']
    mastr_list = power_unit_wind.values.tolist()
    mastr_list = list(dict.fromkeys(mastr_list))
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Windeinheit')

    for i in range(start_from, mastr_list_len, 1):
        unit_wind = get_power_unit_wind(mastr_list[i])
        if unit_wind is not None:
            write_to_csv(fname_wind_unit, unit_wind)
        else:
            log.exception(f'Download failed unit_wind ({i}): {mastr_list[i]}')
            mastr_fail = {'EinheitMastrNummer': [mastr_list[i]]}
            unit_wind_fail = pd.DataFrame(mastr_fail)
            write_to_csv(fname_wind_fail_u, unit_wind_fail)
            #retry_download_unit_wind(unit_wind_fail)


def retry_download_unit_wind(fail_first):
    """Download Windeinheit (unit-wind) after fail.
    This function needs a revision!

    Returns
    -------
    fname_wind_unit : csv
        Write Windeinheit to csv file.
    """
    unit_wind = get_power_unit_wind(fail_first)
    if unit_wind is not None:
        write_to_csv(fname_wind_unit, unit_wind)
    else:
        log.exception(f'Download failed unit_wind ({i}): {mastr_list[i]}')
        mastr_fail = {'EinheitMastrNummer': i}
        fail_second = pd.DataFrame(mastr_fail)
        csv_input = pd.read_csv(fname_wind_fail_u)
        csv_input['2nd Fail'] = fail_second
        csv_input.to_csv(fname_wind_fail_u)


def get_power_unit_wind(mastr_unit_wind):
    """Get Windeinheit (unit-wind) from API using GetEinheitWind.

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
        return unit_wind
    except Exception as e:
        log.info('Download failed for %s', mastr_unit_wind)


def read_unit_wind(csv_name):
    """Encode and read Windeinheit (unit-wind) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind : DataFrame
        Windeinheit.
    """
    if os.path.isfile(csv_name):
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
        unit_wind_cnt = unit_wind['timestamp'].count()
        log.info(f'Read {unit_wind_cnt} Windeinheit from {csv_name}')
        return unit_wind

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-wind-eeg
def download_unit_wind_eeg():
    """Download Windeinheit-EEG (unit-wind-eeg) using GetAnlageEegWind request.

    Filter EegMastrNummer from Stromerzeugungseinheit-Wind.
    Filter EegMastrNummer from Windeinheit.
    Remove duplicates and count.
    Loop over list and write download to file.

    Returns
    -------
    fname_wind_eeg : csv
        Write Windeinheit-EEG to csv file.
    """
    setup_power_unit_wind()

    power_unit_wind_1 = read_power_unit_wind(fname_power_unit_wind)
    power_unit_wind_1 = power_unit_wind_1.dropna(subset=['EegMastrNummer'])
    power_unit_wind_1 = power_unit_wind_1['EegMastrNummer']
    mastr_list_1 = power_unit_wind_1.values.tolist()
    mastr_list_1 = list(dict.fromkeys(mastr_list_1))
    mastr_list_len_1 = len(mastr_list_1)
    log.info(f'Read {mastr_list_len_1} unique EegMastrNummer from Stromerzeugungseinheit-Wind')

    power_unit_wind_2 = read_unit_wind(fname_wind_unit)
    power_unit_wind_2 = power_unit_wind_2.dropna(subset=['EegMastrNummer'])
    power_unit_wind_2 = power_unit_wind_2['EegMastrNummer']
    mastr_list_2 = power_unit_wind_2.values.tolist()
    mastr_list_2 = list(dict.fromkeys(mastr_list_2))
    mastr_list_len_2 = len(mastr_list_2)
    log.info(f'Read {mastr_list_len_2} unique EegMastrNummer from Windeinheit')

    mastr_list = mastr_list_1 + mastr_list_2
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Windeinheit-EEG')

    for i in range(0, mastr_list_len, 1):
        unit_wind_eeg = get_unit_wind_eeg(mastr_list[i])
        if unit_wind_eeg is not None:
            write_to_csv(fname_wind_eeg, unit_wind_eeg)
        else:
            log.exception(f'Download failed unit_wind_eeg ({i}): {mastr_list[i]}')
            mastr_fail = {'EegMastrNummer': [mastr_list[i]]}
            unit_wind_fail = pd.DataFrame(mastr_fail)
            write_to_csv(fname_wind_fail_e, unit_wind_fail)


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
    try:
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
    except Exception as e:
        log.info('Download failed for %s', mastr_wind_eeg)


def read_unit_wind_eeg(csv_name):
    """Encode and read Windeinheit-EEG (unit-wind-eeg) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind_eeg : DataFrame
        Windeinheit-EEG.
    """
    if os.path.isfile(csv_name):
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
        unit_wind_eeg_cnt = unit_wind_eeg['timestamp'].count()
        log.info(f'Read {unit_wind_eeg_cnt} Windeinheit-EEG from {csv_name}')
        return unit_wind_eeg

    else:
        log.info(f'Error reading {csv_name}')


# Download unit-wind-permit
def download_unit_wind_permit():
    """Download Windeinheit-Genehmigung using GetEinheitGenehmigung request.

    Filter GenMastrNummer from Stromerzeugungseinheit-Wind.
    Filter GenMastrNummer from Windeinheit.
    Remove duplicates and count.
    Loop over list and write download to file.

    ToDo: More Documentation needed @solar-c

    Returns
    -------
    fname_wind_permit : csv
        Write Windeinheit-Genehmigung to csv file.
    """
    setup_power_unit_wind()

    power_unit_wind_1 = read_power_unit_wind(fname_power_unit_wind)
    power_unit_wind_1 = power_unit_wind_1.dropna(subset=['GenMastrNummer'])
    power_unit_wind_1 = power_unit_wind_1['GenMastrNummer']
    mastr_list_1 = power_unit_wind_1.values.tolist()
    mastr_list_1 = list(dict.fromkeys(mastr_list_1))
    mastr_list_len_1 = len(mastr_list_1)
    log.info(f'Read {mastr_list_len_1} unique GenMastrNummer from Windeinheit')

    power_unit_wind_2 = read_unit_wind(fname_wind_unit)
    power_unit_wind_2 = power_unit_wind_2.dropna(subset=['GenMastrNummer'])
    power_unit_wind_2 = power_unit_wind_2['GenMastrNummer']
    mastr_list_2 = power_unit_wind_2.values.tolist()
    mastr_list_2 = list(dict.fromkeys(mastr_list_2))
    mastr_list_len_2 = len(mastr_list_2)
    log.info(f'Read {mastr_list_len_2} unique GenMastrNummer from Windeinheit')

    mastr_list = mastr_list_1 + mastr_list_2
    mastr_list = [x for x in mastr_list if str(x) != 'nan']
    mastr_list_len = len(mastr_list)
    log.info(f'Download {mastr_list_len} Windeinheit-Genehmigung')

    df_all = pd.DataFrame()

    for i in range(0, mastr_list_len, 1):
        if not pd.isna(mastr_list[i]):
            try:
                unit_wind_permit = get_unit_wind_permit(mastr_list[i])
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
                    # df_all['version'] = data_version
                    # df_all['timestamp'] = str(datetime.datetime.now())
                    write_to_csv(fname_wind_permit, df_all)
            except:
                log.exception(f'Download failed Windeinheit-Genehmigung ({i}): {mastr_list[i]}')
                mastr_fail = {'GenMastrNummer': [mastr_list[i]]}
                unit_wind_fail = pd.DataFrame(mastr_fail)
                write_to_csv(fname_wind_fail_p, unit_wind_fail)


def get_unit_wind_permit(mastr_wind_permit):
    """Get Windeinheit-Genehmigung from API using GetEinheitGenehmigung.

    Parameters
    ----------
    mastr_wind_permit : str
        Genehmigungnummer MaStR.

    Returns
    -------
    unit_wind_permit : DataFrame
        Windeinheit-Genehmigung.
    """
    data_version = get_data_version()
    try:
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
    except Exception as e:
        log.info('Download failed for %s', mastr_wind_permit)


def read_unit_wind_permit(csv_name):
    """Encode and read Windeinheit-Genehmigung (unit-wind-permit) from CSV file.

    Parameters
    ----------
    csv_name : str
        Name of file.

    Returns
    -------
    unit_wind_permit : DataFrame
        Windeinheit-Genehmigung.
    """
    if os.path.isfile(csv_name):
        unit_wind_permit = pd.read_csv(csv_name, header=0, sep=';', index_col=False, encoding='utf-8',
                                       dtype={
                                              'MaStRNummer': str,
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
        unit_wind_permit = unit_wind_permit.drop('Unnamed: 0', axis=1)
        unit_wind_permit_cnt = unit_wind_permit['MaStRNummer'].count()
        log.info(f'Read {unit_wind_permit_cnt} Windeinheit-Genehmigung from {csv_name}')
        return unit_wind_permit

    else:
        log.info(f'Error reading {csv_name}')


def disentangle_manufacturer(wind_unit):
    """Unfold OrderedDict Hersteller for Windeinheit.

    Returns
    -------
    wind_unit : DataFrame
        Windeinheit with unfold Hersteller.
    """
    wu = wind_unit
    try:
        wu['HerstellerID'] = wind_unit['Hersteller']['Id']
        wu['HerstellerName'] = wind_unit['Hersteller']['Wert']
        return wu
    except:
        print("This wind_unit contains no OrderedDict for 'Hersteller'")
        return wind_unit
