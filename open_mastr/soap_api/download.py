from functools import wraps
from zeep.helpers import serialize_object
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport
import requests
from itertools import product
import logging
import pandas as pd
import multiprocessing
import time
from tqdm import tqdm
from zeep.exceptions import XMLParseError, Fault
import os

from open_mastr.utils import credentials as cred
from open_mastr.soap_api.config import get_filenames, get_project_home_dir, get_data_config, setup_logger


log = setup_logger()

unit_download_title_default = "Download data for  a specific type of " \
                              "energy carrier"
unit_download_desc_default = ""
unit_download_parameter_energy_carrier = (
        '    energy_carrier : str\n'
        '        Retrieve unit data per type of energy carrier.\n\n'
        '        Power plants are grouped to following energy carriers\n\n'
        '        * nuclear\n'
        '        * hydro\n'
        '        * solar\n'
        '        * wind\n'
        '        * biomass\n'
        '        * combustion\n')
unit_download_parameters_common = (
        '    unit_mastr_id : str or list of str, optional\n'
        '        MaStR identifier of a unit that looks like "SME963513379837"\n'
        '        If not given, data for all units is retrieved (considering \n'
        '        for :attr:`.limit`)\n'
        '    limit : int, optional\n'
        '        Limit number of requests. One requests equals downloading of \n'
        '        data of one unit\n')

unit_download_parameters_default = unit_download_parameter_energy_carrier + \
                                   unit_download_parameters_common

unit_doc_template = '\n' \
                    '    {title}\n' \
                    '\n' \
                    '{description}' \
                    '    Parameters\n' \
                    '    ----------\n' \
                    '{parameters}' \
                    '\n' \
                    '    Returns\n' \
                    '    -------\n' \
                    '    Unit data : `dict`\n' \
                    '\n' \
                    '    '


class MaStRAPI(object):
    """
    Access the Marktstammdatenregister (MaStR) SOAP API via a Python wrapper

    `Read about <https://www.marktstammdatenregister.de/MaStRHilfe/index.html>`_
    how to create a user account and a role including a token to access the
    MaStR SOAP API.

    Create an :class:`.MaStRAPI()` instance with your role credentials

    .. code:

        mastr_api = MaStRAPI(
            user="SOM123456789012",
            key=""koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies..."
        )

    Alternatively, leave `user` and `key` empty. Then, you be asked to
    provide your credentials which will be save to the config file
    `~/.open-MaStR/config.ini` (user) and to the keyring (key).

        .. code:

        mastr_api = MaStRAPI()

    Now, you can use the MaStR API instance to call `pre-defined SOAP API
    queries
    <https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Funktionen_MaStR_Webdienste_V1.2.26.html>`_
    via the class' methods.
    For example, get a list of units limited to two entries.

    .. code:

        mastr_api.GetListeAlleEinheiten(limit=2)

    Note, as the example shows, you don't have to pass credentials for calling
    wrapped SOAP queries. This is handled internally.
    """

    def __init__(self, user=None, key=None):
        """
        Parameters
        ----------
        user : str , optional
            MaStR-ID (MaStR-Nummer) for the account that was created on
            https://www.marktstammdatenregister.de
            Typical format: SOM123456789012
        key : str , optional
            Access token of a role (Benutzerrolle). Might look like:
            "koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies..."
        """

        # Bind MaStR SOAP API functions as instance methods
        client, client_bind = _mastr_bindings()

        # First, all services of registered service_port (i.e. 'Anlage')
        for n, f in client_bind:
            setattr(self, n, self._mastr_wrapper(f))

        # Second, general functions like 'GetLokaleUhrzeit'
        for n, f in client.service:
            if n == "GetLokaleUhrzeit":
                setattr(self, n, f)
            else:
                setattr(self, n, self._mastr_wrapper(f))

        # Assign MaStR credentials
        if user:
            self._user = user
        else:
            self._user = cred.get_mastr_user()
        if key:
            self._key = key
        else:
            self._key = cred.get_mastr_token(self._user)

    def _mastr_wrapper(self, soap_func):
        """
        Decorates MaStR SOAP API methods with a wrapper automatically passing
        credentials and serializing return value
        """
        @wraps(soap_func)
        def wrapper(*args, **kwargs):
            kwargs.setdefault("apiKey", self._key)
            kwargs.setdefault("marktakteurMastrNummer", self._user)

            # Catch weird MaStR SOAP response
            try:
                response = soap_func(*args, **kwargs)
            except Fault as e:
                log.warning(f"MaStR SOAP API gives a weird response: {e}. Trying again...")
                time.sleep(1.5)
                try:
                    response = soap_func(*args, **kwargs)
                except Fault as e:
                    msg = f"MaStR SOAP API still gives a weird response: '{e}'.\n"
                                  "We have to stop the program!"
                    log.exception(msg)
                    raise Fault(msg)

            return serialize_object(response, target_cls=dict)

        return wrapper


def _mastr_bindings(max_retries=3,
                    pool_connections=2000,
                    pool_maxsize=2000,
                    timeout=600,
                    wsdl='https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl',
                    service_name='Marktstammdatenregister',
                    service_port='Anlage'):
    """

    Parameters
    ----------
    max_retries : int
        Maximum number of retries for a request. Parameters is passed to
        requests.adapters.HTTPAdapter
    pool_connections : int
        Number of pool connections. Parameters is passed to
        requests.adapters.HTTPAdapter
    pool_maxsize
        Maximum pool size. Parameters is passed to
        requests.adapters.HTTPAdapter
    timeout : int
        Time out in milliseconds. Parameters is passed to
        zeep.transports.Transport
    wsdl : str
        Url of wsdl file to be used. Parameters is passed to zeep.Client
    service_name : str
        Service, defined in wsdl file, that is to be used. Parameters is
        passed to zeep.Client.bind
    service_port : str
        Port of service to be used. Parameters is
        passed to zeep.Client.bind

    Returns
    -------
    zeep.Client : The zeep Client
    zeep.Client.bind : ServiceProxy bindings for given :attr:`service_name`
        and :attr:`service_port`
    """

    wsdl = wsdl
    session = requests.Session()
    session.max_redirects = 30
    a = requests.adapters.HTTPAdapter(
        max_retries=max_retries,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize)
    session.mount('https://', a)
    transport = Transport(cache=SqliteCache(), timeout=timeout, session=session)
    settings = Settings(strict=True, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind(service_name, service_port)

    _mastr_suppress_parsing_errors(['parse-time-second'])

    return client, client_bind


def _mastr_suppress_parsing_errors(which_errors):
    """
    Install logging filters into zeep type parsing modules to suppress

    Arguments
    ---------
    which_errors : [str]
        Names of errors defined in `error_filters` to set up.
        Currently one of ('parse-time-second').

    NOTE
    ----
    zeep and mastr don't seem to agree on the correct time format. Instead of
    suppressing the error, we should fix the parsing error, or they should :).
    """

    class FilterExceptions(logging.Filter):
        def __init__(self, name, klass, msg):
            super().__init__(name)

            self.klass = klass
            self.msg = msg

        def filter(self, record):
            if record.exc_info is None:
                return 1

            kl, inst, tb = record.exc_info
            return 0 if isinstance(inst, self.klass) and inst.args[0] == self.msg else 1

    # Definition of available filters
    error_filters = [FilterExceptions('parse-time-second', ValueError, 'second must be in 0..59')]

    # Install filters selected by `which_errors`
    zplogger = logging.getLogger('zeep.xsd.types.simple')
    zplogger.filters = ([f for f in zplogger.filters if not isinstance(f, FilterExceptions)] +
                        [f for f in error_filters if f.name in which_errors])


def _flatten_dict(data):
    """
    Flattens MaStR data dictionary to depth of one

    Parameters
    ----------
    data : list of dict
        Data returned from MaStR-API query

    Returns
    -------
    list of dict
        Flattened data dictionary
    """

    # The rule describes which of the second-level keys are used to replace first-level data
    flatten_rule_replace = {
        'Hausnummer': "Wert",
        "Kraftwerksnummer": "Wert",
        "Weic": "Wert",
        "WeitereBrennstoffe": "Wert",
        "WeitererHauptbrennstoff": "Wert",
        "AnlagenkennzifferAnlagenregister": "Wert",
        "VerhaeltnisErtragsschaetzungReferenzertrag": "Wert",
        "VerhaeltnisReferenzertragErtrag10Jahre": "Wert",
        "VerhaeltnisReferenzertragErtrag15Jahre": "Wert",
        "VerhaeltnisReferenzertragErtrag5Jahre": "Wert",
        "RegistrierungsnummerPvMeldeportal": "Wert",
        "BiogasGaserzeugungskapazitaet": "Wert",
        "BiomethanErstmaligerEinsatz": "Wert",
        "Frist": "Wert",
        "WasserrechtAblaufdatum": "Wert",
    }

    flatten_rule_replace_list = {
        "VerknuepfteEinheit": "MaStRNummer",
        "VerknuepfteEinheiten": "MaStRNummer"
    }

    flatten_rule_expand = {
        "Ertuechtigung": "Id"}

    flatten_rule_move_up_and_merge = ["Hersteller"]

    flatten_rule_none_if_empty_list = ["ArtDerFlaeche",
                                       "WeitereBrennstoffe",
                                       "VerknuepfteErzeugungseinheiten"]

    for dic in data:
        # Replacements with second-level values
        for k, v in flatten_rule_replace.items():
            if k in dic.keys():
                dic[k] = dic[k][v]

        # Replacement with second-level value from second-level list
        for k, v in flatten_rule_replace_list.items():
            if k in dic.keys():
                dic[k] = dic[k][0][v]

        # Expands multiple entries (via list items) to separate new columns (might explode)
        # Assumes a list of dicts below key (example: Ertuechtigung in hydro power)
        for k, v in flatten_rule_expand.items():
            if k in dic.keys():
                dic.update({k + "_number": len(dic[k])})
                for sub_data in dic[k]:
                    sub_data_id = sub_data[v]
                    for k_sub_data, v_sub_data in sub_data.items():
                        if k_sub_data != v:
                            dic.update({k_sub_data + "_" + sub_data_id: v_sub_data})

                # Remove original data
                dic.pop(k)

        # Join 'Id' with original key to new column
        # and overwrite original data with 'Wert'
        for k in flatten_rule_move_up_and_merge:
            if k in dic.keys():
                dic.update({k + "Id": dic[k]["Id"]})
                dic.update({k: dic[k]["Wert"]})

        # Avoid empty lists as values
        for k in flatten_rule_none_if_empty_list:
            if k in dic.keys():
                if dic[k] == []:
                    dic[k] = None
                else:
                    dic[k] = ",".join(dic[k])

    return data


def to_csv(df, technology):
    DATA_VERSION = get_data_config()["data_version"]
    DATA_PATH = os.path.join(get_project_home_dir(), "data", DATA_VERSION)
    filenames = get_filenames()

    csv_file = os.path.join(DATA_PATH, filenames["raw"][technology]["joined"])

    df.to_csv(csv_file, index=True, index_label="EinheitMastrNummer", encoding='utf-8')


def _missed_units_to_file(technology, data_type, missed_units):
    """
    Write IDs of missed units to file

    Parameters
    ----------
    technology : str
        Technology, see :meth:`MaStRDownload.download_power_plants`
    data_type : str
        Which type of additional data. Options: 'extended', 'eeg', 'kwk', 'permit'
    missed_units : list
        Unit IDs of missed data
    """

    DATA_VERSION = get_data_config()["data_version"]
    DATA_PATH = os.path.join(get_project_home_dir(), "data", DATA_VERSION)
    filenames = get_filenames()
    missed_units_file = os.path.join(DATA_PATH, filenames["raw"][technology][f"{data_type}_fail"])

    with open(missed_units_file, 'w') as f:
        for item in missed_units:
            f.write(f"{item}\n")


class _MaStRDownloadFactory(type):
    def __new__(cls, name, bases, dct):
        # Assign factory properties to concrete object
        x = super().__new__(cls, name, bases, dct)

        # Assign mastr_api
        x._mastr_api = MaStRAPI()

        return x


class MaStRDownload(metaclass=_MaStRDownloadFactory):
    """Use the higher level interface of MaStRDownload to download unit data

    :class:`.MaStRDownload` builds on top of :class:`.MaStRAPI()` and provides
    an interface for easier downloading.
    Use methods documented below to retrieve specific data. On the example of
    data for nuclear power plants, this looks like

    .. code-block:: python

        from open_mastr.soap_api.download import MaStRDownload

        mastr_dl = MaStRDownload()
        mastr_dl.nuclear()

    This downloads power plant unit data for all nuclear power plants
    registered in MaStR.

    Note
    ----
    Be careful with download data without limit. This might exceed your daily
    contigent of requests!

    """

    def __init__(self):

        # Specify which additional data for each unit type is available
        # and which SOAP service has to be used to query it
        self._unit_data_specs = {
            "biomass": {
                "unit_data": "GetEinheitBiomasse",
                "energietraeger": ["Biomasse"],
                "kwk_data": "GetAnlageKwk",
                "eeg_data": "GetAnlageEegBiomasse",
                "permit_data": "GetEinheitGenehmigung"

            },
            "combustion": {
                "unit_data": "GetEinheitVerbrennung",
                "energietraeger": ["Steinkohle", "Braunkohle", "Erdgas", "AndereGase", "Mineraloelprodukte",
                                   "NichtBiogenerAbfall", "Waerme"],
                "kwk_data": "GetAnlageKwk",
                "permit_data": "GetEinheitGenehmigung"
            },
            "gsgk": {
                "unit_data": "GetEinheitGeoSolarthermieGrubenKlaerschlammDruckentspannung",
                "energietraeger": ["Geothermie", "Solarthermie", "Grubengas", "Klaerschlamm"],
                "kwk_data": "GetAnlageKwk",
                "eeg_data": "GetAnlageEegGeoSolarthermieGrubenKlaerschlammDruckentspannung",
                "permit_data": "GetEinheitGenehmigung"
            },
            "nuclear": {
                "unit_data": "GetEinheitKernkraft",
                "energietraeger": ["Kernenergie"],
                "permit_data": "GetEinheitGenehmigung"
            },
            "solar": {
                "unit_data": "GetEinheitSolar",
                "energietraeger": ["SolareStrahlungsenergie"],
                "eeg_data": "GetAnlageEegSolar",
                "permit_data": "GetEinheitGenehmigung"
            },
            "wind": {
                "unit_data": "GetEinheitWind",
                "energietraeger": ["Wind"],
                "eeg_data": "GetAnlageEegWind",
                "permit_data": "GetEinheitGenehmigung"
            },
            "hydro": {
                "unit_data": "GetEinheitWasser",
                "energietraeger": ["Wasser"],
                "eeg_data": "GetAnlageEegWasser",
                "permit_data": "GetEinheitGenehmigung"
            },
        }

    def download_power_plants(self, technology, limit=None):
        """

        Parameters
        ----------
        technology
        limit

        Returns
        -------
        pd.DataFrame
            Joined data tables
        """

        # Retrieve basic power plant unit data
        units = self._basic_unit_data(technology, limit)

        # Prepare list of unit ID for different additional data (extended, eeg, kwk, permit)
        mastr_ids = [basic['EinheitMastrNummer'] for basic in units]

        # Prepare list of EEG data unit IDs
        if "eeg_data" in self._unit_data_specs[technology].keys():
            eeg_ids = [basic['EegMastrNummer'] for basic in units if basic['EegMastrNummer']]
        else:
            eeg_ids = []

        # Prepare list of KWK data unit IDs
        if "kwk_data" in self._unit_data_specs[technology].keys():
            kwk_ids = [basic['KwkMastrNummer'] for basic in units if basic['KwkMastrNummer']]
        else:
            kwk_ids = []

        # Prepare list of permit data unit IDs
        if "permit_data" in self._unit_data_specs[technology].keys():
            permit_ids = [basic['GenMastrNummer'] for basic in units if basic['GenMastrNummer']]
        else:
            permit_ids = []

        # Download additional data for unit
        extended_data, extended_missed = self._additional_data(technology, mastr_ids, "_extended_unit_data")
        eeg_data, eeg_missed = self._additional_data(technology, eeg_ids, "_eeg_unit_data")
        kwk_data, kwk_missed = self._additional_data(technology, kwk_ids, "_kwk_unit_data")
        permit_data, permit_missed = self._additional_data(technology, permit_ids, "_permit_unit_data")


        # Retry missed additional unit data
        if extended_missed:
            extended_data_retry, extended_missed_retry = self._retry_missed_additional_data(
                technology,
                extended_missed,
                "_extended_unit_data")
            extended_data.extend(extended_data_retry)
            _missed_units_to_file(technology, "extended", extended_missed_retry)
        if eeg_missed:
            eeg_data_retry, eeg_missed_retry = self._retry_missed_additional_data(
                technology,
                eeg_missed,
                "_eeg_unit_data")
            eeg_data.extend(eeg_data_retry)
            _missed_units_to_file(technology, "eeg", eeg_missed_retry)
        if kwk_missed:
            kwk_data_retry, kwk_missed_retry = self._retry_missed_additional_data(
                technology,
                kwk_missed,
                "_kwk_unit_data")
            kwk_data.extend(kwk_data_retry)
            _missed_units_to_file(technology, "kwk", kwk_missed_retry)
        if permit_missed:
            permit_data_retry, permit_missed_retry = self._retry_missed_additional_data(
                technology,
                permit_missed,
                "_permit_unit_data")
            permit_data.extend(permit_data_retry)
            _missed_units_to_file(technology, "permit", permit_missed_retry)

        # Flatten data
        extended_data = _flatten_dict(extended_data)
        eeg_data = _flatten_dict(eeg_data)
        kwk_data = _flatten_dict(kwk_data)
        permit_data = _flatten_dict(permit_data)

        # Join data to a single dataframe
        idx_cols = [(units, "EinheitMastrNummer", ""),
                    (extended_data, "EinheitMastrNummer", "_unit"),
                    (eeg_data, "VerknuepfteEinheit", "_eeg"),
                    (kwk_data, "VerknuepfteEinheiten", "_kwk"),
                    (permit_data, "VerknuepfteEinheiten", "_permit")
                    ]

        joined_data = pd.DataFrame(idx_cols[0][0]).set_index(idx_cols[0][1])

        for dat, idx_col, suf in idx_cols[1:]:
            # Make sure at least on non-empty dict is in dat
            if any(dat):
                joined_data = joined_data.join(pd.DataFrame(dat).set_index(idx_col), rsuffix=suf)

        # Remove duplicates
        joined_data.drop_duplicates(inplace=True)

        to_csv(joined_data, technology)

        return joined_data

    def _basic_unit_data(self, technology, limit):

        for et in self._unit_data_specs[technology]["energietraeger"]:
            log.info(f"Get list of units with basic information for technology {technology} ({et})")
            units = self._mastr_api.GetGefilterteListeStromErzeuger(
                energietraeger=et,
                limit=limit)["Einheiten"]
        return units

    def _additional_data(self, technology, unit_ids, data_fcn):
        """
        Retrieve addtional informations about units.

        Extended information on units is available. Depending on type, additional data from EEG and KWK subsidy program
        are available. Furthermore, for some units, data about permit is retrievable.

        Parameters
        ----------
        technology : str
            Technology, see :meth:`MaStRDownload.download_power_plants`
        unit_ids : list
            Unit identifier for additional data
        data_fcn : str
            Name of method from :class:`MaStRDownload` to be used for querying additional data

        Returns
        -------
        tuple of list of dict or str
            Returns additional data in dictionaries that are packed into a list. Format

            .. code-block:: python

               return = (
                    [additional_unit_data_dict1, additional_unit_data_dict2, ...],
                    [missed_unit1, missed_unit2, ...]
                    )
        """
        prepared_args = list(product(unit_ids, [technology]))

        with multiprocessing.Pool() as pool:

            # Apply extended data download functions
            data_tmp = []

            # Download data unit data
            for args in prepared_args:
                data_tmp.append(pool.apply_async(self.__getattribute__(data_fcn), args))
                # time.sleep(0.1)

            # Retrieve data
            data = []
            data_missed = []
            for res, unit_id in tqdm(zip(data_tmp, unit_ids),
                                     total=len(prepared_args),
                                     desc=f"Downloading{data_fcn} ({technology})".replace("_", " "),
                                     unit="unit"):
                try:
                    data.append(res.get(timeout=1))
                except (requests.exceptions.ConnectionError, multiprocessing.context.TimeoutError) as e:
                    log.debug(f"Connection aborted: {e}")
                    data_missed.append(unit_id)

        return data, data_missed

    def _extended_unit_data(self, mastr_id, technology):
        # print(f"Downoading additional unit data for {mastr_id} (technology: {specs})")

        try:
            unit_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[technology]["unit_data"])(einheitMastrNummer=mastr_id)
        except XMLParseError as e:
            log.exception(
                f"Failed to download unit data for {mastr_id} because of SOAP API exception: {e}",
                exc_info=False)
            unit_data = {}

        return unit_data

    def _eeg_unit_data(self, eeg_id, technology):

        eeg_data = self._mastr_api.__getattribute__(
            self._unit_data_specs[technology]["eeg_data"])(eegMastrNummer=eeg_id)

        return eeg_data

    def _kwk_unit_data(self, kwk_id, technology):

        kwk_data = self._mastr_api.__getattribute__(
            self._unit_data_specs[technology]["kwk_data"])(kwkMastrNummer=kwk_id)

        return kwk_data

    def _permit_unit_data(self, permit_id, technology):
        permit_data = self._mastr_api.__getattribute__(
            self._unit_data_specs[technology]["permit_data"])(genMastrNummer=permit_id)

        return permit_data

    def _retry_missed_additional_data(self, technology, missed_ids, data_fcn, retries=3):

        log.info(f"Retrying to download additional data for {len(missed_ids)} "
                 f"{technology} units with {retries} retries")

        data = []

        missed_ids_remaining = missed_ids
        for retry in range(1, retries + 1):
            data_tmp, missed_ids_tmp = self._additional_data(
                technology, missed_ids_remaining, data_fcn)
            if data_tmp:
                data.extend(data_tmp)
            missed_ids_remaining = missed_ids_tmp

            if not missed_ids_remaining:
                break

        return data, missed_ids_remaining


if __name__ == "__main__":
    pass

# TODO: Pass through kargs to _unit_data() that are possible to use with GetGefilterteListeStromErzeuger() and mention in docs
