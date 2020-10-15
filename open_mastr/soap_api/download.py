from functools import wraps
from zeep.helpers import serialize_object
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport
import requests
import logging

from open_mastr.utils import credentials as cred


log = logging.getLogger(__name__)


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
        for n, f in client_bind:
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
