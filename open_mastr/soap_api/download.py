import logging
import time
from functools import wraps
import requests
from open_mastr.utils import credentials as cred
from open_mastr.utils.config import (
    setup_logger,
)
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.exceptions import Fault
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
            key="koo5eixeiQuoi'w8deighai8ahsh1Ha3eib3coqu7ceeg%ies...",
            service_port="Anlage"
       )
    ```

    Alternatively, leave `user` and `key` empty if user and token are accessible via
    `credentials.cfg`. How to configure this is described
    [here](../advanced.md/#mastr-account-and-credentials).

    ```python

        mastr_api = MaStRAPI()
    ```

    Now, you can use the MaStR API instance to call pre-defined SOAP API
    queries via the class' methods. A documentation of all API methods
    is available at the
    [BNetzA website](https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html)
    within the downloadable zip folder `Dienstbeschreibung Produktion Version X.X.X`
    For example, get a list of units limited to two entries.

    ```python

       mastr_api.GetListeAlleEinheiten(limit=2)
    ```

    !!! Note
        As the example shows, you don't have to pass credentials for calling
        wrapped SOAP queries. This is handled internally.
    """

    def __init__(self, user=None, key=None, service_port="Anlage"):
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
        service_port : str , optional
            Port/model to be used, e.g. "Anlage" or "Akteur", see docs for
            full list:
            https://www.marktstammdatenregister.de/MaStRHilfe/subpages/webdienst.html
            Defaults to "Anlage".
        """

        # Bind MaStR SOAP API functions as instance methods
        client, client_bind = _mastr_bindings(service_port=service_port)

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
    service_port,
    service_name="Marktstammdatenregister",
    wsdl="https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl",
    max_retries=3,
    pool_connections=100,
    pool_maxsize=100,
    timeout=60,
    operation_timeout=600,
):
    """

    Parameters
    ----------
    service_port : str
        Port of service to be used. Parameters is passed to `zeep.Client.bind`
        See :class:`MaStRAPI` for more information.
    service_name : str
        Service, defined in wsdl file, that is to be used. Parameters is
        passed to zeep.Client.bind
    wsdl : str
        Url of wsdl file to be used. Parameters is passed to zeep.Client
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
