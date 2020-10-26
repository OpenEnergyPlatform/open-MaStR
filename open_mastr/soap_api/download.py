from functools import wraps
from zeep.helpers import serialize_object
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport
import requests
import logging
import pandas as pd

from open_mastr.utils import credentials as cred


log = logging.getLogger(__name__)

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

UNIT_SPECS = {
    "biomass": {
        "unit_data": "GetEinheitBiomasse",
        "energietraeger": ["Biomasse"],
        "docs": {
            "title": "Download biomass power plant unit data",
            "parameters": unit_download_parameters_default},
        "kwk_data": "GetAnlageKwk",
        "eeg_data": "GetAnlageEegWind"

    },
    "combustion": {
        "unit_data": "GetEinheitVerbrennung",
        "energietraeger": ["Steinkohle", "Braunkohle", "Erdgas", "AndereGase", "Mineraloelprodukte", "NichtBiogenerAbfall", "Waerme"],
        "docs": {
            "title": "Download conventional power plant unit data",
            "parameters": unit_download_parameters_default},
        "kwk_data": "GetAnlageKwk",
    },
    "gsgk": {
        "unit_data": "GetEinheitGeoSolarthermieGrubenKlaerschlamm",
        "energietraeger": ["Geothermie", "Solarthermie", "Grubengas", "Klaerschlamm"],
        "docs": {
            "title": "Download geo and other power plant unit data",
            "parameters": unit_download_parameters_default},
        "kwk_data": "GetAnlageKwk",
        "eeg_data": "GetAnlageEegGeoSolarthermieGrubenKlaerschlamm"
    },
    "nuclear": {
        "unit_data": "GetEinheitKernkraft",
        "energietraeger": ["Kernenergie"],
        "docs": {
            "title": "Download nuclear power plant unit data",
            "parameters": unit_download_parameters_default,
        }
    },
    "solar": {
        "unit_data": "GetEinheitSolar",
        "energietraeger": ["SolareStrahlungsenergie"],
        "docs": {
            "title": "Download PV power plant unit data",
            "parameters": unit_download_parameters_default},
        "eeg_data": "GetAnlageEegSolar"
    },
    "wind": {
        "unit_data": "GetEinheitWind",
        "energietraeger": ["Wind"],
        "docs": {
            "title": "Download wind power plant unit data",
            "parameters": unit_download_parameters_default},
        "eeg_data": "GetAnlageEegWind"
    },
    "hydro": {
        "unit_data": "GetEinheitWasser",
        "energietraeger": ["Wasser"],
        "docs": {
            "title": "Download hydro power plant unit data",
            "parameters": unit_download_parameters_default},
        "eeg_data": "GetAnlageEegWasser"
    },
}


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
            return serialize_object(soap_func(*args, **kwargs), target_cls=dict)

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


def _build_docstring(ec):
    unit_docstring = unit_doc_template.format(
        title=UNIT_SPECS[ec]["docs"]["title"],
        description=UNIT_SPECS[ec]["docs"].get("description", ""),
        parameters=unit_download_parameters_common
    )
    return unit_docstring


def _unit_data_wrapper(unit_data_func, mastr_api, name):
    """
    Wraps around _unit_data() and passes parameters for each unit type
    """

    @wraps(unit_data_func)
    def unit_data_creator(*args, **kwargs):
        # return unit_data_func(*args, **kwargs)
        return unit_data_func(mastr_api, name, **kwargs)

    return unit_data_creator


def _unit_data(mastr_api, energy_carrier, unit_mastr_id=[], eeg=True, limit=None):
    """
    Download data for  a specific type of energy carrier

    Parameters
    ----------
    mastr_api : :class:`.MaStRAPI()`
        Low-level download API
    energy_carrier : str
        Retrieve unit data per type of energy carrier.
    unit_mastr_id : str, optional
    limit : str, optional

    Returns
    -------

    """

    # In case no unit mastr id is specified, get list of all units and download data
    if not unit_mastr_id:
        # units = []
        for et in UNIT_SPECS[energy_carrier]["energietraeger"]:
            units = mastr_api.GetGefilterteListeStromErzeuger(
                energietraeger=et,
                limit=limit)["Einheiten"]

        unit_mastr_id_dict = {}
        for unit in units:
            unit_mastr_id_dict[unit["EinheitMastrNummer"]] = {}

            # Save foreign keys for extra data retrieval
            if "eeg_data" in UNIT_SPECS[energy_carrier]:
                unit_mastr_id_dict[unit["EinheitMastrNummer"]]["eeg"] = unit["EegMastrNummer"]
            elif "kwk_data" in UNIT_SPECS[energy_carrier]:
                unit_mastr_id_dict[unit["EinheitMastrNummer"]]["kwk"] = unit["KwkMastrNummer"]

    # In case a single unit is requested by user
    if not isinstance(unit_mastr_id, list):
        unit_mastr_id = list(unit_mastr_id_dict.keys())

    # Get unit data
    unit_data, eeg_data, kwk_data = _retrieve_additional_unit_data(unit_mastr_id_dict, energy_carrier, mastr_api)

    # Flatten dictionaries
    # TODO: use existing code in soap_api/

    # merge data
    unit_data_df = pd.DataFrame(unit_data).set_index("EinheitMastrNummer")

    # return unit_data
    return unit_data, eeg_data, kwk_data


def _retrieve_additional_unit_data(units, energy_carrier, mastr_api):
    """
    Retrieve addtional informations about units

    Extended information on units is available. Depending on type, additional data from EEG and KWK subsidy program
    are available. Furthermore, for wind energy converters, data about permit is retrievable.

    Parameters
    ----------
    units : dict
        Keyed by MaStR-Nummer with optional sub-keys 'eeg', 'kwk', 'permit'. Each having the respective identifier as 
        value
    energy_carrier : str
        Retrieve unit data per type of energy carrier.
    mastr_api : :class:`.MaStRAPI()`
        Low-level download API

    Returns
    -------
    tuple of list of dict
        Returns additional data in dictionaries that are packed into a tuple. Format
        
        .. code-block:: python
        
           return = (
                [additional_unit_data_dict1, additional_unit_data_dict2, ...],
                [eeg_unit_data_dict1, eeg_unit_data_dict2, ...],
                [kwk_unit_data_dict1, kwk_unit_data_dict2, ...],
                [permit_unit_data_dict1, permit_unit_data_dict2, ...]
                )
    """
    # Get unit data
    unit_data = []
    eeg_data = []
    kwk_data = []
    for unit_id, data_ext in units.items():
        unit_data.append(mastr_api.__getattribute__(UNIT_SPECS[energy_carrier]["unit_data"])(einheitMastrNummer=unit_id))

        # Get unit EEG data
        if "eeg" in data_ext.keys() and data_ext["eeg"] is not None:
            eeg_data.append(mastr_api.__getattribute__(UNIT_SPECS[energy_carrier]["eeg_data"])(
                eegMastrNummer=data_ext["eeg"])
            )
        else:
            log.info("No EEG data available for unit type {}".format(energy_carrier))

        # Get unit KWK data
        if "kwk" in data_ext.keys() and data_ext["kwk"] is not None:
            kwk_data.append(mastr_api.__getattribute__(UNIT_SPECS[energy_carrier]["kwk_data"])(
                kwkMastrNummer=data_ext["kwk"])
            )
        else:
            log.info("No KWK data available for unit type {}".format(energy_carrier))

    return unit_data, eeg_data, kwk_data


class _MaStRDownloadFactory(type):
    def __new__(cls, name, bases, dct):
        # Assign factory properties to concrete object
        x = super().__new__(cls, name, bases, dct)

        # Assign mastr_api
        x._mastr_api = MaStRAPI()

        for ec in UNIT_SPECS.keys():
            # Define methods
            setattr(x, ec, _unit_data_wrapper(_unit_data, x._mastr_api, ec))

            # Define docstring for method
            docstring = _build_docstring(ec)
            getattr(x, ec).__doc__ = docstring

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
        """Init docstring"""
        pass


if __name__ == "__main__":
    pass


# TODO: Pass through kargs to _unit_data() that are possible to use with GetGefilterteListeStromErzeuger() and mention in docs
