import json
import logging
import multiprocessing
import os
import time
from functools import wraps
from itertools import product

import pandas as pd
import requests
from open_mastr.utils import credentials as cred
from open_mastr.utils.config import (
    create_data_dir,
    get_data_version_dir,
    get_filenames,
    setup_logger,
)
from tqdm import tqdm
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.exceptions import Fault, XMLParseError
from zeep.helpers import serialize_object
from zeep.transports import Transport

log = setup_logger()


class MaStRAPI(object):
    """
    Access the Marktstammdatenregister (MaStR) SOAP API via a Python wrapper

    Read about [MaStR account and credentials](../advanced.md/#mastr-account-and-credentials)
    how to create a user account and a role including a token to access the
    MaStR SOAP API.

    Create an `MaStRAPI` instance with your role credentials

    ```python

       mastr_api = MaStRAPI(
            user="SOM123456789012",
            key=""koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies..."
       )
    ```

    Alternatively, leave `user` and `key` empty if user and token are accessible via
    `credentials.cfg`. How to configure this is described
    [here](../advanced.md/#mastr-account-and-credentials).

    ```python

        mastr_api = MaStRAPI()
    ```

    Now, you can use the MaStR API instance to call [pre-defined SOAP API
    queries](https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Funktionen_MaStR_Webdienste_V1.2.39.html)
    via the class' methods.
    For example, get a list of units limited to two entries.

    ```python

       mastr_api.GetListeAlleEinheiten(limit=2)
    ```

    !!! Note
        As the example shows, you don't have to pass credentials for calling
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

        self._user = user if user else cred.get_mastr_user()
        self._key = key if key else cred.get_mastr_token(self._user)

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
            except Fault:
                time.sleep(1.5)
                try:
                    response = soap_func(*args, **kwargs)
                except Fault as e:
                    msg = (
                        (
                            f"MaStR SOAP API still gives a weird response: '{e}'.\n"
                            "Retry failed!"
                        )
                        if e.message != "Zugriff verweigert"
                        else (
                            "Your credentials could not be used to "
                            "access the MaStR SOAP API from BNetzA. Please make sure that "
                            "they are correct."
                        )
                    )
                    raise Fault(msg) from e

            return serialize_object(response, target_cls=dict)

        return wrapper


def _mastr_bindings(
    max_retries=3,
    pool_connections=100,
    pool_maxsize=100,
    timeout=60,
    operation_timeout=600,
    wsdl="https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl",
    service_name="Marktstammdatenregister",
    service_port="Anlage",
):
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
        Timeout for loading wsdl sfn xsd documents in seconds. Parameter
        is passed to `zeep.transports.Transport`.
    operation_timeout : int
        Timeout for API requests (GET/POST in underlying requests package)
        in seconds. Parameter is passed to `zeep.transports.Transport`.
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
        pool_maxsize=pool_maxsize,
    )
    session.mount("https://", a)
    transport = Transport(
        cache=SqliteCache(),
        timeout=timeout,
        operation_timeout=operation_timeout,
        session=session,
    )
    settings = Settings(strict=False, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind(service_name, service_port)

    _mastr_suppress_parsing_errors(["parse-time-second"])

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
    error_filters = [
        FilterExceptions("parse-time-second", ValueError, "second must be in 0..59")
    ]

    # Install filters selected by `which_errors`
    zplogger = logging.getLogger("zeep.xsd.types.simple")
    zplogger.filters = [
        f for f in zplogger.filters if not isinstance(f, FilterExceptions)
    ] + [f for f in error_filters if f.name in which_errors]


def replace_second_level_keys_with_first_level_data(dic: dict) -> dict:
    """The returned dict from the API call often contains nested dicts. The
    columns where this happens are defined in flatten_rule_replace. The nested dicts
    are replaced by its actual value.

    Example:
    "WasserrechtAblaufdatum": {"Wert": None, "NichtVorhanden": False}
    -> "WasserrechtAblaufdatum": None


    Parameters
    ----------
    dic : dict
        Dictionary containing information on single unit

    Returns
    -------
    dict
        Dictionary where nested entries are replaced by data of interest
    """
    flatten_rule_replace = {
        "Hausnummer": "Wert",
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
    for k, v in flatten_rule_replace.items():
        if k in dic:
            dic[k] = dic[k][v]

    return dic


def replace_linked_units_with_unit_identifier(dic: dict) -> dict:
    """If data point in 'dic' has one or more VerknuepfteEinheit or
    VerknuepfteEinheiten in its respective dict, the related units (VerknuepfteEinheiten) are inserted as comma-separated strings.

    Parameters
    ----------
    dic : dict
        Dictionary containing information on single unit

    Returns
    -------
    dict
        Dictionary where linked units are replaced with linked unit identifier (MaStRNummer)
    """

    flatten_rule_replace_list = {
        "VerknuepfteEinheit": "MaStRNummer",
        "VerknuepfteEinheiten": "MaStRNummer",
        "Netzanschlusspunkte": "NetzanschlusspunktMastrNummer",
    }
    for k, v in flatten_rule_replace_list.items():
        if k in dic:
            if len(dic[k]) != 0:
                mastr_nr_list = [unit[v] for unit in dic[k]]
                dic[k] = ", ".join(mastr_nr_list)
            else:  # in case list is emtpy
                dic[k] = ""
    return dic


def replace_entries_of_type_list(dic: dict) -> dict:
    """Entries that are lists are replaced:
    Empty lists are replaced with None.
    Lists of strings are replaced with comma seperated strings.

    Parameters
    ----------
    dic : dict
        Dictionary containing information on single unit

    Returns
    -------
    dict
        Dictionary containing information on single unit without list entries.
    """
    flatten_rule_none_if_empty_list = [
        "ArtDerFlaeche",
        "WeitereBrennstoffe",
        "VerknuepfteErzeugungseinheiten",
    ]
    for k in flatten_rule_none_if_empty_list:
        if k in dic:
            dic[k] = None if dic[k] == [] else ",".join(dic[k])
    return dic


def flatten_dict(data: list, serialize_with_json: bool = False) -> list:
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
    flatten_rule_serialize = ["Ertuechtigung"]

    flatten_rule_move_up_and_merge = ["Hersteller"]

    for dic in data:
        dic = replace_second_level_keys_with_first_level_data(dic)
        dic = replace_linked_units_with_unit_identifier(dic)

        # Serilializes dictionary entries with unknown number of sub-entries into JSON string
        # This affects "Ertuechtigung" in extended unit data of hydro
        if serialize_with_json:
            for k in flatten_rule_serialize:
                if k in dic.keys():
                    dic[k] = json.dumps(dic[k], indent=4, sort_keys=True, default=str)

        # Join 'Id' with original key to new column
        # and overwrite original data with 'Wert'
        for k in flatten_rule_move_up_and_merge:
            if k in dic.keys():
                dic.update({k + "Id": dic[k]["Id"]})
                dic.update({k: dic[k]["Wert"]})

        dic = replace_entries_of_type_list(dic)
    return data


def _missed_units_to_file(data, data_type, missed_units):
    """
    Write IDs of missed units to file

    Parameters
    ----------
    data : str
        Data, see :meth:`MaStRDownload.download_power_plants`
    data_type : str
        Which type of additional data. Options: 'extended', 'eeg', 'kwk', 'permit'
    missed_units : list
        Unit IDs of missed data
    """

    data_path = get_data_version_dir()
    filenames = get_filenames()
    missed_units_file = os.path.join(
        data_path, filenames["raw"][data][f"{data_type}_fail"]
    )

    with open(missed_units_file, "w") as f:
        for i, error in missed_units:
            f.write(f"{i},{error}\n")


class MaStRDownload:
    """
    !!! warning

        **This class is deprecated** and will not be maintained from version 0.15.0 onwards.
        Instead use [`Mastr.download`][open_mastr.Mastr.download] with parameter
        `method` = "bulk" to get bulk downloads of the dataset.

    Use the higher level interface for bulk download

    `MaStRDownload` builds on top of [`MaStRAPI`][open_mastr.soap_api.download.MaStRAPI] and provides
    an interface for easier downloading.
    Use methods documented below to retrieve specific data. On the example of
    data for nuclear power plants, this looks like

    ```python

        from open_mastr.soap_api.download import MaStRDownload

        mastr_dl = MaStRDownload()

        for tech in ["nuclear", "hydro", "wind", "solar", "biomass", "combustion", "gsgk"]:
            power_plants = mastr_dl.download_power_plants(tech, limit=10)
            print(power_plants.head())
    ```

    !!! warning

        Be careful with increasing `limit`. Typically, your account allows only for 10,000 API
        request per day.

    """

    def __init__(self, parallel_processes=None):
        """

        Parameters
        ----------
        parallel_processes : int or bool, optional
            Specify number of parallel unit data download, respectively
            the number of processes you want to use for downloading.
            For single-process download (avoiding the use of python
            multiprocessing package) choose False.
            Defaults to number of cores (including hyperthreading).
        """
        log.warn(
            """
            The `MaStRDownload` class is deprecated and will not be maintained in the future.
            To get a full table of the Marktstammdatenregister, use the open_mastr.Mastr.download
            method.

            If this change causes problems for you, please comment in this issue on github:
            https://github.com/OpenEnergyPlatform/open-MaStR/issues/487

            """
        )

        # Number of parallel processes
        if parallel_processes == "max":
            self.parallel_processes = multiprocessing.cpu_count()
        else:
            self.parallel_processes = parallel_processes

        # Specify which additional data for each unit type is available
        # and which SOAP service has to be used to query it
        self._unit_data_specs = {
            "biomass": {
                "unit_data": "GetEinheitBiomasse",
                "energietraeger": ["Biomasse"],
                "kwk_data": "GetAnlageKwk",
                "eeg_data": "GetAnlageEegBiomasse",
                "permit_data": "GetEinheitGenehmigung",
            },
            "combustion": {
                "unit_data": "GetEinheitVerbrennung",
                "energietraeger": [
                    "Steinkohle",
                    "Braunkohle",
                    "Erdgas",
                    "AndereGase",
                    "Mineraloelprodukte",
                    "NichtBiogenerAbfall",
                    "Waerme",
                ],
                "kwk_data": "GetAnlageKwk",
                "permit_data": "GetEinheitGenehmigung",
            },
            "gsgk": {
                "unit_data": "GetEinheitGeothermieGrubengasDruckentspannung",
                "energietraeger": [
                    "Geothermie",
                    "Solarthermie",
                    "Grubengas",
                    "Klaerschlamm",
                ],
                "kwk_data": "GetAnlageKwk",
                "eeg_data": "GetAnlageEegGeothermieGrubengasDruckentspannung",
                "permit_data": "GetEinheitGenehmigung",
            },
            "nuclear": {
                "unit_data": "GetEinheitKernkraft",
                "energietraeger": ["Kernenergie"],
                "permit_data": "GetEinheitGenehmigung",
            },
            "solar": {
                "unit_data": "GetEinheitSolar",
                "energietraeger": ["SolareStrahlungsenergie"],
                "eeg_data": "GetAnlageEegSolar",
                "permit_data": "GetEinheitGenehmigung",
            },
            "wind": {
                "unit_data": "GetEinheitWind",
                "energietraeger": ["Wind"],
                "eeg_data": "GetAnlageEegWind",
                "permit_data": "GetEinheitGenehmigung",
            },
            "hydro": {
                "unit_data": "GetEinheitWasser",
                "energietraeger": ["Wasser"],
                "eeg_data": "GetAnlageEegWasser",
                "permit_data": "GetEinheitGenehmigung",
            },
            "storage": {
                "unit_data": "GetEinheitStromSpeicher",
                "energietraeger": ["Speicher"],
                "eeg_data": "GetAnlageEegSpeicher",
                # todo: additional data request not created for permit, create manually
                "permit_data": "GetEinheitGenehmigung",
            },
            "gas_storage": {
                "unit_data": "GetEinheitGasSpeicher",
                "energietraeger": ["Speicher"],
            },
            # TODO: unsure if energietraeger Ergdas makes sense
            "gas_consumer": {
                "unit_data": "GetEinheitGasVerbraucher",
                "energietraeger": ["Erdgas"],
            },
            "electricity_consumer": {
                "unit_data": "GetEinheitStromVerbraucher",
                "energietraeger": ["Strom"],
            },
            "gas_producer": {
                "unit_data": "GetEinheitGasErzeuger",
                "energietraeger": [None],
            },
            "location_elec_generation": "GetLokationStromErzeuger",
            "location_elec_consumption": "GetLokationStromVerbraucher",
            "location_gas_generation": "GetLokationGasErzeuger",
            "location_gas_consumption": "GetLokationGasVerbraucher",
        }

        # Map additional data to primary key via data_fcn
        self._additional_data_primary_key = {
            "extended_unit_data": "EinheitMastrNummer",
            "kwk_unit_data": "KwkMastrNummer",
            "eeg_unit_data": "EegMastrNummer",
            "permit_unit_data": "GenMastrNummer",
            "location_data": "MastrNummer",
        }

        # Check if MaStR credentials are available and otherwise ask
        # for user input
        self._mastr_api = MaStRAPI()
        self._mastr_api._user = cred.check_and_set_mastr_user()
        self._mastr_api._key = cred.check_and_set_mastr_token(self._mastr_api._user)

    def download_power_plants(self, data, limit=None):
        """
        Download power plant unit data for one data type.

        Based on list with basic information about each unit, subsequently additional
        data is retrieved:

        * Extended unit data
        * EEG data is collected during support of renewable energy installations
          by the Erneuerbare-Energie-Gesetz.
        * KWK data is collected to the support program Kraft-Waerme-Kopplung
        * Permit data is available for some installations (German: Genehmigungsdaten)

        Data is stored in CSV file format in `~/open-MaStR/data/<version>/` by
        default.

        Parameters
        ----------
        data : str
            Retrieve unit data for one power system unit. Power plants are
            grouped by following technologies:

            * 'nuclear'
            * 'hydro'
            * 'solar'
            * 'wind'
            * 'biomass'
            * 'combustion'
            * 'gsgk'
            * 'storage'
        limit : int
            Maximum number of units to be downloaded.

        Returns
        -------
        pd.DataFrame
            Joined data tables.
        """
        # Create data version directory
        create_data_dir()

        # Check requests contingent
        self.daily_contingent()

        # Retrieve basic power plant unit data
        # The return value is casted into a list, because a generator gets returned
        # This was introduced later, after creation of this method
        units = [
            unit
            for sublist in self.basic_unit_data(data=data, limit=limit)
            for unit in sublist
        ]

        # Prepare list of unit ID for different additional data (extended, eeg, kwk, permit)
        mastr_ids = self._create_ID_list(units, "unit_data", "EinheitMastrNummer", data)
        eeg_ids = self._create_ID_list(units, "eeg_data", "EegMastrNummer", data)
        kwk_ids = self._create_ID_list(units, "kwk_data", "KwkMastrNummer", data)
        permit_ids = self._create_ID_list(units, "permit_data", "GenMastrNummer", data)

        # Download additional data for unit
        extended_data, extended_missed = self.additional_data(
            data, mastr_ids, "extended_unit_data"
        )
        if eeg_ids:
            eeg_data, eeg_missed = self.additional_data(data, eeg_ids, "eeg_unit_data")
        else:
            eeg_data = eeg_missed = []
        if kwk_ids:
            kwk_data, kwk_missed = self.additional_data(data, kwk_ids, "kwk_unit_data")
        else:
            kwk_data = kwk_missed = []
        if permit_ids:
            permit_data, permit_missed = self.additional_data(
                data, permit_ids, "permit_unit_data"
            )
        else:
            permit_data = permit_missed = []

        # Retry missed additional unit data
        if extended_missed:
            (
                extended_data_retry,
                extended_missed_retry,
            ) = self._retry_missed_additional_data(
                data, [_[0] for _ in extended_missed], "extended_unit_data"
            )
            extended_data.extend(extended_data_retry)
            _missed_units_to_file(data, "extended", extended_missed_retry)
        if eeg_missed:
            eeg_data_retry, eeg_missed_retry = self._retry_missed_additional_data(
                data, [_[0] for _ in eeg_missed], "eeg_unit_data"
            )
            eeg_data.extend(eeg_data_retry)
            _missed_units_to_file(data, "eeg", eeg_missed_retry)
        if kwk_missed:
            kwk_data_retry, kwk_missed_retry = self._retry_missed_additional_data(
                data, [_[0] for _ in kwk_missed], "kwk_unit_data"
            )
            kwk_data.extend(kwk_data_retry)
            _missed_units_to_file(data, "kwk", kwk_missed_retry)
        if permit_missed:
            permit_data_retry, permit_missed_retry = self._retry_missed_additional_data(
                data, [_[0] for _ in permit_missed], "permit_unit_data"
            )
            permit_data.extend(permit_data_retry)
            _missed_units_to_file(data, "permit", permit_missed_retry)

        # Flatten data
        extended_data = flatten_dict(extended_data, serialize_with_json=True)
        eeg_data = flatten_dict(eeg_data, serialize_with_json=True)
        kwk_data = flatten_dict(kwk_data, serialize_with_json=True)
        permit_data = flatten_dict(permit_data, serialize_with_json=True)

        # Join data to a single dataframe
        idx_cols = [
            (units, "EinheitMastrNummer", ""),
            (extended_data, "EinheitMastrNummer", "_unit"),
            (eeg_data, "VerknuepfteEinheit", "_eeg"),
            (kwk_data, "VerknuepfteEinheiten", "_kwk"),
            (permit_data, "VerknuepfteEinheiten", "_permit"),
        ]

        joined_data = pd.DataFrame(idx_cols[0][0]).set_index(idx_cols[0][1])

        for dat, idx_col, suf in idx_cols[1:]:
            # Make sure at least on non-empty dict is in dat
            if any(dat):
                joined_data = joined_data.join(
                    pd.DataFrame(dat).set_index(idx_col), rsuffix=suf
                )

        # Remove duplicates
        joined_data.drop_duplicates(inplace=True)

        to_csv(joined_data, data)  # FIXME: reference to helpers im import

        return joined_data

    def _create_ID_list(self, units, data_descriptor, key, data):
        """Extracts a list of MaStR numbers (eeg, kwk, or permit Mastr Nr) from the given units."""
        return (
            [basic[key] for basic in units if basic[key]]
            if data_descriptor in self._unit_data_specs[data]
            else []
        )

    def basic_unit_data(self, data=None, limit=2000, date_from=None, max_retries=3):
        """
        Download basic unit information for one data type.

        Retrieves basic information about units. The number of unit in
        bound to `limit`.

        Parameters
        ----------
        data : str, optional
            Technology data is requested for. See :meth:`MaStRDownload.download_power_plants`
            for options.
            Data is retrieved using `MaStRAPI.GetGefilterteListeStromErzeuger`.
            If not given, it defaults to `None`. This implies data for all available
            technologies is retrieved using the web service function
            `MaStRAPI.GetListeAlleEinheiten`.
        limit : int, optional
            Maximum number of units to download.
            If not provided, data for all units is downloaded.

            !!! warning
                Mind the daily request limit for your MaStR account.
        date_from: `datetime.datetime()`, optional
            If specified, only units with latest change date newer than this are queried.
            Defaults to `None`.
        max_retries: int, optional
            Maximum number of retries in case of errors with the connection to the server.

        Yields
        ------
        list of dict
            A generator of dicts is returned with each dictionary containing
            information about one unit.
        """
        # Split download of basic unit data in chunks of 2000
        # Reason: the API limits retrieval of data to 2000 items
        chunksize = 2000
        chunks_start = list(range(1, limit + 1, chunksize))
        limits = [
            chunksize if (x + chunksize) <= limit else limit - x + 1
            for x in chunks_start
        ]

        # Deal with or w/o data type being specified
        energietraeger = (
            self._unit_data_specs[data]["energietraeger"] if data else [None]
        )

        # In case multiple energy carriers (energietraeger) exist for one data type,
        # loop over these and join data to one list
        for et in energietraeger:
            log.info(
                f"Get list of units with basic information for data type {data} ({et})"
            )
            yield from (
                basic_data_download(
                    self._mastr_api,
                    "GetListeAlleEinheiten",
                    "Einheiten",
                    chunks_start,
                    limits,
                    date_from,
                    max_retries,
                    data,
                    et=et,
                )
                if et is None
                else basic_data_download(
                    self._mastr_api,
                    "GetGefilterteListeStromErzeuger",
                    "Einheiten",
                    chunks_start,
                    limits,
                    date_from,
                    max_retries,
                    data,
                    et=et,
                )
            )

    def additional_data(self, data, unit_ids, data_fcn, timeout=10):
        """
        Retrieve addtional informations about units.

        Extended information on units is available. Depending on type, additional data from EEG
        and KWK subsidy program are available.
        Furthermore, for some units, data about permit is retrievable.

        Parameters
        ----------
        data : str
            data, see :meth:`MaStRDownload.download_power_plants`
        unit_ids : list
            Unit identifier for additional data
        data_fcn : str
            Name of method from :class:`MaStRDownload` to be used for querying additional data.
            Choose from

            * "extended_unit_data" (:meth:`~.extended_unit_data`): Extended information
              (i.e. technical, location)
              about a unit. The exact set of information depends on the data type.
            * "eeg_unit_data" (:meth:`~.eeg_unit_data`): Unit Information from
              EEG unit registry. The exact
              set of information depends on the data.
            * "kwk_unit_data" (:meth:`~.kwk_unit_data`): Unit information from KWK unit registry.
            * "permit_unit_data" (:meth:`~.permit_unit_data`): Information about the permit
              process of a unit.
        timeout: int, optional
            Timeout limit for data retrieval for each unit when using multiprocessing. Defaults to 10.

        Returns
        -------
        tuple of list of dict and tuple
            Returns additional data in dictionaries that are packed into a list.
            ```python
                return = (
                    [additional_unit_data_dict1, additional_unit_data_dict2, ...],
                    [tuple("SME930865355925", "Reason for failing dowload"), ...]
                    )
            ```
        """
        # Prepare a list of unit IDs packed as tuple associated with data
        prepared_args = list(product(unit_ids, [data]))

        # Prepare results lists

        if self.parallel_processes:
            data, data_missed = self._retrieve_data_in_parallel_process(
                prepared_args, data_fcn, data, timeout
            )
        else:
            data, data_missed = self._retrieve_data_in_single_process(
                prepared_args, data_fcn, data
            )

        # Remove Nones and empty dicts
        data = [dat for dat in data if dat]
        data_missed = [dat for dat in data_missed if dat]

        # Add units missed due to timeout to data_missed
        units_retrieved = [_[self._additional_data_primary_key[data_fcn]] for _ in data]
        units_missed_timeout = [
            (u, "Timeout")
            for u in unit_ids
            if u not in units_retrieved + [_[0] for _ in data_missed]
        ]
        data_missed += units_missed_timeout

        return data, data_missed

    def _retrieve_data_in_single_process(self, prepared_args, data_fcn, data):
        data_list = []
        data_missed_list = []
        for unit_specs in tqdm(
            prepared_args,
            total=len(prepared_args),
            desc=f"Downloading {data_fcn} ({data})",
            unit="unit",
        ):
            data_tmp, data_missed_tmp = self.__getattribute__(data_fcn)(unit_specs)
            if not data_tmp:
                log.debug(
                    f"Download for additional data for "
                    f"{data_missed_tmp[0]} ({data}) failed. "
                    f"Traceback of caught error:\n{data_missed_tmp[1]}"
                )
            data_list.append(data_tmp)
            data_missed_list.append(data_missed_tmp)

        return data_list, data_missed_list

    def _retrieve_data_in_parallel_process(
        self, prepared_args, data_fcn, data, timeout
    ):
        data_list = []
        data_missed_list = []
        with multiprocessing.Pool(
            processes=self.parallel_processes, maxtasksperchild=1
        ) as pool:
            with tqdm(
                total=len(prepared_args),
                desc=f"Downloading {data_fcn} ({data})",
                unit="unit",
            ) as pbar:
                unit_result = pool.imap_unordered(
                    self.__getattribute__(data_fcn), prepared_args, chunksize=1
                )
                while True:
                    try:
                        # Try to retrieve data from concurrent processes
                        data_tmp, data_missed_tmp = unit_result.next(timeout=timeout)

                        if not data_tmp:
                            log.debug(
                                f"Download for additional data for "
                                f"{data_missed_tmp[0]} ({data}) failed. "
                                f"Traceback of caught error:\n{data_missed_tmp[1]}"
                            )
                        data_list.append(data_tmp)
                        data_missed_list.append(data_missed_tmp)
                        pbar.update()
                    except StopIteration:
                        # Multiprocessing returns StropIteration when results list gets empty
                        break
                    except multiprocessing.TimeoutError:
                        # If retrieval time exceeds timeout of next(), pass on
                        log.debug(f"Data request for 1 {data} unit timed out")
        return data_list, data_missed_list

    def extended_unit_data(self, unit_specs):
        """
        Download extended data for a unit.

        This extended unit information is provided separately.

        Parameters
        ----------
        unit_specs : tuple
            *EinheitMastrNummer* and data type as tuple that for example looks like

            ```python

               tuple("SME930865355925", "hydro")
            ```

        Returns
        -------
        dict
            Extended information about unit, if download successful,
            otherwise empty dict
        tuple
            *EinheitMastrNummer* and message the explains why a download failed. Format

            ```python

               tuple("SME930865355925", "Reason for failing dowload")
            ```
        """

        mastr_id, data = unit_specs
        try:
            unit_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[data]["unit_data"]
            )(einheitMastrNummer=mastr_id)
            unit_missed = None
        except (
            XMLParseError,
            Fault,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as e:
            # log.exception(
            #     f"Failed to download unit data for {mastr_id} because of SOAP API exception: {e}",
            #     exc_info=False)
            unit_data = {}
            unit_missed = (mastr_id, repr(e))

        return unit_data, unit_missed

    def eeg_unit_data(self, unit_specs):
        """
        Download EEG (Erneuerbare Energien Gesetz) data for a unit.

        Additional data collected during a subsidy program for supporting
        installations of renewable energy power plants.

        Parameters
        ----------
        unit_specs : tuple
            *EegMastrnummer* and data type as tuple that for example looks like

            .. code-block:: python

               tuple("EEG961554380393", "hydro")

        Returns
        -------
        dict
            EEG details about unit, if download successful,
            otherwise empty dict
        tuple
            *EegMastrNummer* and message the explains why a download failed. Format

            ```python

               tuple("EEG961554380393", "Reason for failing dowload")
            ```
        """
        eeg_id, data = unit_specs
        try:
            eeg_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[data]["eeg_data"]
            )(eegMastrNummer=eeg_id)
            eeg_missed = None
        except (
            XMLParseError,
            Fault,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as e:
            # log.exception(
            #     f"Failed to download eeg data for {eeg_id} because of SOAP API exception: {e}",
            #     exc_info=False)
            eeg_data = {}
            eeg_missed = (eeg_id, repr(e))

        return eeg_data, eeg_missed

    def kwk_unit_data(self, unit_specs):
        """
        Download KWK (german: Kraft-Wärme-Kopplung, english: Combined Heat and Power, CHP)
        data for a unit.

        Additional data collected during a subsidy program for supporting
        combined heat power plants.

        Parameters
        ----------
        unit_specs : tuple
            *KwkMastrnummer* and data type as tuple that for example looks like

            ```python

               tuple("KWK910493229164", "biomass")
            ```


        Returns
        -------
        dict
            KWK details about unit, if download successful,
            otherwise empty dict
        tuple
            *KwkMastrNummer* and message the explains why a download failed. Format

            ```python

               tuple("KWK910493229164", "Reason for failing dowload")
            ```
        """
        kwk_id, data = unit_specs
        try:
            kwk_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[data]["kwk_data"]
            )(kwkMastrNummer=kwk_id)
            kwk_missed = None
        except (
            XMLParseError,
            Fault,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as e:
            # log.exception(
            #     f"Failed to download unit data for {kwk_id} because of SOAP API exception: {e}",
            #     exc_info=False)
            kwk_data = {}
            kwk_missed = (kwk_id, repr(e))

        return kwk_data, kwk_missed

    def permit_unit_data(self, unit_specs):
        """
        Download permit data for a unit.

        Parameters
        ----------
        unit_specs : tuple
            *GenMastrnummer* and data type as tuple that for example looks like

            ```python

               tuple("SGE952474728808", "biomass")
            ```


        Returns
        -------
        dict
            Permit details about unit, if download successful,
            otherwise empty dict
        tuple
            *GenMastrNummer* and message the explains why a download failed. Format

            ```python

               tuple("GEN952474728808", "Reason for failing dowload")
            ```
        """
        permit_id, data = unit_specs
        try:
            permit_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[data]["permit_data"]
            )(genMastrNummer=permit_id)
            permit_missed = None
        except (
            XMLParseError,
            Fault,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as e:
            # log.exception(
            #     f"Failed to download unit data for {permit_id} "
            #     f"because of SOAP API exception: {e}",
            #     exc_info=False)
            permit_data = {}
            permit_missed = (permit_id, repr(e))

        return permit_data, permit_missed

    def location_data(self, specs):
        """
        Download extended data for a location

        Allows to download additional data for different location types, see *specs*.

        Parameters
        ----------
        specs : tuple
            Location *Mastrnummer* and data_name as tuple that for example looks like

            ```python

               tuple("SEL927688371072", "location_elec_generation")
            ```


        Returns
        -------
        dict
            Detailed information about a location, if download successful,
            otherwise empty dict
        tuple
            Location *MastrNummer* and message the explains why a download failed. Format

            ```python

               tuple("SEL927688371072", "Reason for failing dowload")
            ```
        """

        # Unpack tuple argument to two separate variables
        location_id, data_name = specs

        try:
            data = self._mastr_api.__getattribute__(self._unit_data_specs[data_name])(
                lokationMastrNummer=location_id
            )
            missed = None
        except (
            XMLParseError,
            Fault,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ) as e:
            data = {}
            missed = (location_id, repr(e))

        return data, missed

    def _retry_missed_additional_data(self, data, missed_ids, data_fcn, retries=3):
        """
        Retry to download extended data that was missed earlier.

        Tries three times (default) to download data.

        Parameters
        ----------
        data : str
            data, see :meth:`MaStRDownload.download_power_plants`
        missed_ids : list
            Unit identifiers for additional data
        data_fcn : str
            Name of method from :class:`MaStRDownload` to be used for querying additional data
        retries : int
            Number of retries (default: 3).

        Returns
        -------
        tuple of lists
            Queried data and still missed unit IDs are returned as :code:`(data, missed_units)`.
        """

        log.info(
            f"Retrying to download additional data for {len(missed_ids)} "
            f"{data} units with {retries} retries"
        )

        data = []

        missed_ids_remaining = missed_ids
        for _ in range(1, retries + 1):
            data_tmp, missed_ids_tmp = self.additional_data(
                data, missed_ids_remaining, data_fcn
            )
            if data_tmp:
                data.extend(data_tmp)
            missed_ids_remaining = [_[0] for _ in missed_ids_tmp]

            if not any(missed_ids_remaining):
                break

        return data, missed_ids_tmp

    def basic_location_data(self, limit=2000, date_from=None, max_retries=3):
        """
        Retrieve basic location data in chunks

        Retrieves data for all types of locations at once using
        `MaStRAPI.GetListeAlleLokationen`.
        Locations include

        * Electricity generation location (SEL - Stromerzeugungslokation)
        * Electricity consumption location (SVL - Stromverbrauchslokation)
        * Gas generation location (GEL - Gaserzeugungslokation)
        * Gas consumption location (GVL - Gasverbrauchslokation)

        Parameters
        ----------
        limit: int, optional
            Maximum number of locations to download.

            !!! warning
                Mind the daily request limit for your MaStR account, usually 10,000 per day.
        date_from: `datetime.datetime`, optional
            If specified, only locations with latest change date newer than this are queried.
        max_retries: int, optional
            Maximum number of retries for each chunk in case of errors with the connection to
            the server.

        Yields
        ------
        generator of generators
            For each chunk a separate generator is returned all wrapped into another generator.
            Access with

            ```python

                chunks = mastr_dl.basic_location_data(
                    date_from=datetime.datetime(2020, 11, 7, 0, 0, 0), limit=2010
                    )

                for chunk in chunks:
                    for location in chunk:
                        print(location) # prints out one dict per location one after another
            ```
        """
        # Prepare indices for chunked data retrieval
        chunksize = 2000
        chunks_start = list(range(1, limit + 1, chunksize))
        limits = [
            chunksize if (x + chunksize) <= limit else limit - x + 1
            for x in chunks_start
        ]

        yield from basic_data_download(
            self._mastr_api,
            "GetListeAlleLokationen",
            "Lokationen",
            chunks_start,
            limits,
            date_from,
            max_retries,
        )

    def daily_contingent(self):
        contingent = self._mastr_api.GetAktuellerStandTageskontingent()
        log.info(
            f"Daily requests contigent: "
            f"{contingent['AktuellerStandTageskontingent']} "
            f"/ {contingent['AktuellesLimitTageskontingent']}"
        )


def basic_data_download(
    mastr_api,
    fcn_name,
    category,
    chunks_start,
    limits,
    date_from,
    max_retries,
    data=None,
    et=None,
):
    """
    Helper function for downloading basic data with MaStR list query

    Helps to query data from list-returning functions like GetListeAlleEinheiten.
    Automatically

    * respects limit of 2.000 rows returned by MaStR list functions
    * stops, if no further data is available
    * nicely integrates dynamic update of tqdm progress bar

    Parameters
    ----------
    mastr_api: :class:`MaStRAPI`
        MaStR API wrapper
    fcn_name: str
        Name of list-returning download function
    category: str
        Either "Einheiten" or "Lokationen"
    chunks_start: list of int
        Start index for each chunk. MaStR data is internally index by an integer index that can
        be used to start querying data from a certain position. Here, it is used to
        iterate over the available data.
    limits: list of int
        Limit of queried row for each chunk.
    date_from: datetime.datetime
        Date for querying only newer data than this date
    max_retries: int
        Number of maximum retries for each chunk
    data: str, optional
        Choose a subset from available technologies. Only relevant if category="Einheiten".
        Defaults to all technologies.
    et: str
        Energietraeger of a data type. Some technologies are subdivided into a list of
        energietraeger. Only relevant if category="Einheiten". Defaults to None.

    Yields
    ------
    list
        Basic unit or location information. Depends on category.
    """

    # Construct description string
    description = f"Get basic {category} data information"
    if data:
        description += f" for data {data}"
    if et:
        description += f" ({et})"

    pbar = tqdm(desc=description, unit=" units")

    # Iterate over chunks and download data
    # Results are first collected per 'et' (units_tech) for properly
    # displaying download progress.
    # Later, all units of a single data are collected in 'units'
    for chunk_start, limit_iter in zip(chunks_start, limits):
        # Use a retry loop to retry on connection errors
        for try_number in range(max_retries + 1):
            try:
                if et is None:
                    response = getattr(mastr_api, fcn_name)(
                        startAb=chunk_start, limit=limit_iter, datumAb=date_from
                    )
                else:
                    response = getattr(mastr_api, fcn_name)(
                        energietraeger=et,
                        startAb=chunk_start,
                        limit=limit_iter,
                        datumAb=date_from,
                    )

            except (
                requests.exceptions.ConnectionError,
                Fault,
                requests.exceptions.ReadTimeout,
            ) as e:
                try_number += 1
                log.debug(
                    f"MaStR SOAP API does not respond properly: {e}. Retry {try_number}"
                )
                time.sleep(5)
            else:
                # If it does run into the except clause, break out of the for loop
                # This also means query was successful
                units_tech = response[category]
                yield units_tech
                pbar.update(len(units_tech))
                break
        else:
            log.error(
                f"Finally failed to download data."
                f"Basic unit data of index {chunk_start} to "
                f"{chunk_start + limit_iter - 1} will be missing."
            )
            # TODO: this has potential risk! Please change
            # If the download continuously fails on the last chunk, this query will run forever
            response = {"Ergebniscode": "OkWeitereDatenVorhanden"}

        # Stop querying more data, if no further data available
        if response["Ergebniscode"] == "OkWeitereDatenVorhanden":
            continue

        # Update progress bar and move on with next et or data type
        pbar.total = pbar.n
        pbar.refresh()
        pbar.close()
        break

    # Make sure progress bar is closed properly
    pbar.close()


if __name__ == "__main__":
    pass
