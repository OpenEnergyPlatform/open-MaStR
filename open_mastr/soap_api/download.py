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
import math

from open_mastr.utils import credentials as cred
from open_mastr.soap_api.config import get_filenames, get_project_home_dir, get_data_config, \
    setup_logger, get_data_version_dir, create_data_dir


log = setup_logger()


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
                    msg = (f"MaStR SOAP API still gives a weird response: '{e}'.\n"
                                  "We have to stop the program!")
                    log.exception(msg)
                    raise Fault(msg)

            return serialize_object(response, target_cls=dict)

        return wrapper


def _mastr_bindings(max_retries=3,
                    pool_connections=100,
                    pool_maxsize=100,
                    timeout=10,
                    operation_timeout=30,
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
        pool_maxsize=pool_maxsize)
    session.mount('https://', a)
    transport = Transport(cache=SqliteCache(),
                          timeout=timeout,
                          operation_timeout=operation_timeout,
                          session=session)
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
    data_path = get_data_version_dir()
    filenames = get_filenames()

    csv_file = os.path.join(data_path, filenames["raw"][technology]["joined"])

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

    data_path = get_data_version_dir()
    filenames = get_filenames()
    missed_units_file = os.path.join(data_path, filenames["raw"][technology][f"{data_type}_fail"])

    with open(missed_units_file, 'w') as f:
        for item in missed_units:
            f.write(f"{item}\n")


def _chunksize(length, processes):
    """
    Estimate a useful chunksize for parallel download.

    Depends on the list `length` and the number fo `processes`.

    Parameters
    ----------
    length : int
        Length of list items to be downloaded
    processes : int
        Number of parallel processes

    Returns
    -------
    int
        Chunksize
    """
    if processes > 1:
        chunksize = int(math.ceil(length / (processes * 20)))
    else:
        chunksize = 1

    return chunksize

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

        power_plants = mastr_dl.download_power_plants("nuclear", limit=10)
        print(power_plants.head())

    This downloads power plant unit data for all nuclear power plants
    registered in MaStR.

    Note
    ----
    Be careful with download data without limit. This might exceed your daily
    contigent of requests!

    """

    def __init__(self, parallel_processes=multiprocessing.cpu_count()):
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

        # Number of parallel processes
        self.parallel_processes = parallel_processes

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
        Download power plant unit data for one technology.

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
        technology : str
            Retrieve unit data for one power system unit. Power plants are
            grouped by following technologies:

            * 'nuclear'
            * 'hydro'
            * 'solar'
            * 'wind'
            * 'biomass'
            * 'combustion'
            * 'gsgk'

        limit : int
            Maximum number of units to be downloaded. Defaults to :code:`None`.

        Returns
        -------
        pd.DataFrame
            Joined data tables
        """
        # Create data version directory
        create_data_dir()

        # Check requests contingent
        self.daily_contingent()

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
        if eeg_ids:
            eeg_data, eeg_missed = self._additional_data(technology, eeg_ids, "_eeg_unit_data")
        else:
            eeg_data = eeg_missed = []
        if kwk_ids:
            kwk_data, kwk_missed = self._additional_data(technology, kwk_ids, "_kwk_unit_data")
        else:
            kwk_data = kwk_missed = []
        if permit_ids:
            permit_data, permit_missed = self._additional_data(technology, permit_ids, "_permit_unit_data")
        else:
            permit_data = permit_missed = []


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
        """
        Download basic unit information for one technology.

        Retrieves basic information about units. The number of unit in
        bound to `limit`.

        Parameters
        ----------
        technology : str
            Technology, see :meth:`MaStRDownload.download_power_plants`
        limit : int
            Maximum number of units to download.

        Returns
        -------
        list of dict
            A list of dicts is returned with each dictionary containing
            information about one unit.
        """
        # Split download of basic unit data in chunks of 2000
        # Reason: the API limits retrieval of data to 2000 items
        chunksize = 2000
        chunks_start = list(range(1, limit + 1, chunksize))
        limits = [chunksize if (x + chunksize) <= limit
                  else limit - x + 1 for x in chunks_start]

        units = []
        # In case multiple energy carriers (energietraeger) exist for one technology,
        # loop over these and join data to one list
        for et in self._unit_data_specs[technology]["energietraeger"]:
            log.info(f"Get list of units with basic information for technology {technology} ({et})")

            units_tech = []

            pbar = tqdm(total=limit,
                        desc=f"Get list of units with basic information for technology {technology} ({et})",
                        unit=" units")

            # Iterate over chunks and download data
            # Results are first collected per 'et' (units_tech) for properly
            # displaying download progress.
            # Later, all units of a single technology are collected in 'units'
            for chunk_start, limit_iter in zip(chunks_start, limits):
                response = self._mastr_api.GetGefilterteListeStromErzeuger(
                    energietraeger=et,
                    startAb=chunk_start,
                    limit=limit_iter)
                units_tech.extend(response["Einheiten"])
                pbar.update(len(response["Einheiten"]))

                # Stop querying more data, if no further data available
                if response["Ergebniscode"] == 'OkWeitereDatenVorhanden':
                    continue
                else:
                    # Update progress bar and move on with next et or technology
                    pbar.total = len(units_tech)
                    pbar.refresh()
                    pbar.close()
                    break

            # Make sure progress bar is closed properly
            pbar.close()

            # Put units of 'technology' in units list
            units.extend(units_tech)

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
        # Prepare a list of unit IDs packed as tuple associated with technology
        prepared_args = list(product(unit_ids, [technology]))

        # Prepare results lists
        data = []
        data_missed = []


        if self.parallel_processes:
            # Estimate a suitable chunksize
            chunksize = _chunksize(len(prepared_args), self.parallel_processes)

            # Open a pool of workers and retrieve data in parallel
            with multiprocessing.Pool(processes=self.parallel_processes,
                                      maxtasksperchild=1) as pool:

                for unit_result in tqdm(pool.imap_unordered(self.__getattribute__(data_fcn),
                                                            prepared_args,
                                                            chunksize=chunksize),
                                        total=len(prepared_args),
                                        desc=f"Downloading{data_fcn} ({technology})".replace("_", " "),
                                        unit="unit"):
                    data_tmp, data_missed_tmp = unit_result
                    data.append(data_tmp)
                    data_missed.append(data_missed_tmp)
        else:
            # Retrieve data in a single process
            for unit_id in tqdm(unit_ids,
                                     total=len(prepared_args),
                                     desc=f"Downloading{data_fcn} ({technology})".replace("_", " "),
                                     unit="unit"):
                try:
                    data.append(self.__getattribute__(data_fcn)(unit_id, technology))
                except (requests.exceptions.ConnectionError, multiprocessing.context.TimeoutError) as e:
                    log.debug(f"Connection aborted: {e}")
                    data_missed.append(unit_id)

        # Remove Nones and empty dicts
        data = [dat for dat in data if dat]
        data_missed = [dat for dat in data_missed if dat]

        return data, data_missed

    def _extended_unit_data(self, unit_specs):
        """
        Download extended data for a unit.

        This extended unit information is provided separately.

        Parameters
        ----------
        unit_specs : tuple
            *EinheitMastrNummer* and technology as tuple that for example looks like

            .. code-block:: python

               tuple("SME930865355925", "hydro")

        Returns
        -------
        dict
            Extended information about unit, if download successful,
            otherwise empty dict
        str
            *EinheitMastrNummer*, if download failed, otherwise None
        """

        mastr_id, technology = unit_specs
        try:
            unit_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[technology]["unit_data"])(einheitMastrNummer=mastr_id)
            unit_missed = None
        except (XMLParseError, Fault, requests.exceptions.ConnectionError) as e:
            log.exception(
                f"Failed to download unit data for {mastr_id} because of SOAP API exception: {e}",
                exc_info=False)
            unit_data = {}
            unit_missed = mastr_id

        return unit_data, unit_missed

    def _eeg_unit_data(self, unit_specs):
        """
        Download EEG (Erneuerbare Energien Gesetz) data for a unit.

        Additional data collected during a subsidy program for supporting
        installations of renewable energy power plants.

        Parameters
        ----------
        unit_specs : tuple
            *EegMastrnummer* and technology as tuple that for example looks like

            .. code-block:: python

               tuple("EEG961554380393", "hydro")

        Returns
        -------
        dict
            EEG details about unit, if download successful,
            otherwise empty dict
        str
            *EegMastrNummer*, if download failed, otherwise None
        """
        # TODO: Update docstring to change arguments
        eeg_id, technology = unit_specs
        try:
            eeg_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[technology]["eeg_data"])(eegMastrNummer=eeg_id)
            eeg_missed = None
        except (XMLParseError, Fault, requests.exceptions.ConnectionError) as e:
            log.exception(
                f"Failed to download eeg data for {eeg_id} because of SOAP API exception: {e}",
                exc_info=False)
            eeg_data = {}
            eeg_missed = eeg_id

        return eeg_data, eeg_missed

    def _kwk_unit_data(self, unit_specs):
        """
        Download KWK (Kraft-Wärme-Kopplung) data for a unit.

        Additional data collected during a subsidy program for supporting
        combined heat power plants.

        Parameters
        ----------
        unit_specs : tuple
            *KwkMastrnummer* and technology as tuple that for example looks like

            .. code-block:: python

               tuple("KWK910493229164", "biomass")


        Returns
        -------
        Returns
        -------
        dict
            KWK details about unit, if download successful,
            otherwise empty dict
        str
            *EegMastrNummer*, if download failed, otherwise None
        """
        kwk_id, technology = unit_specs
        try:
            kwk_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[technology]["kwk_data"])(kwkMastrNummer=kwk_id)
            kwk_missed = None
        except (XMLParseError, Fault, requests.exceptions.ConnectionError) as e:
            log.exception(
                f"Failed to download unit data for {kwk_id} because of SOAP API exception: {e}",
                exc_info=False)
            kwk_data = {}
            kwk_missed = kwk_id

        return kwk_data, kwk_missed

    def _permit_unit_data(self, unit_specs):
        """
        Download permit data for a unit.

        Parameters
        ----------
        unit_specs : tuple
            *GenMastrnummer* and technology as tuple that for example looks like

            .. code-block:: python

               tuple("SGE952474728808", "biomass")


        Returns
        -------
        dict
            Permit details about unit, if download successful,
            otherwise empty dict
        str
            *GenMastrNummer*, if download failed, otherwise None
        """
        permit_id, technology = unit_specs
        try:
            permit_data = self._mastr_api.__getattribute__(
                self._unit_data_specs[technology]["permit_data"])(genMastrNummer=permit_id)
            permit_missed = None
        except (XMLParseError, Fault, requests.exceptions.ConnectionError) as e:
            log.exception(
                f"Failed to download unit data for {permit_id} because of SOAP API exception: {e}",
                exc_info=False)
            permit_data = {}
            permit_missed = permit_id

        return permit_data, permit_missed

    def _retry_missed_additional_data(self, technology, missed_ids, data_fcn, retries=3):
        """
        Retry to download extended data that was missed earlier.

        Tries three times (default) to download data.

        Parameters
        ----------
        technology : str
            Technology, see :meth:`MaStRDownload.download_power_plants`
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

    def daily_contingent(self):
        contingent = self._mastr_api.GetAktuellerStandTageskontingent()
        log.info(f"Daily requests contigent: "
                 f"{contingent['AktuellerStandTageskontingent']} "
                 f"/ {contingent['AktuellesLimitTageskontingent']}")


if __name__ == "__main__":
    pass

# TODO: Pass through kargs to _unit_data() that are possible to use with GetGefilterteListeStromErzeuger() and mention in docs
