## MaStR account and credentials


For downloading data from the
[Marktstammdatenregister (MaStR) database](https://www.marktstammdatenregister.de/MaStR)
via its API a [registration](https://www.marktstammdatenregister.de/MaStRHilfe/files/regHilfen/201108_Handbuch%20f%C3%BCr%20Registrierungen%20durch%20Dienstleister.pdf) is mandatory.

To download data from the MaStR API using the `open-MaStR`, the credentials (MaStR user and token) need to be provided in a certain way. Three options exist:

1. **Credentials file:** 
    Both, user and token, are stored in plain text in the credentials file.
    For storing the credentials in the credentials file (plus optionally using keyring for the token) simply instantiate
    [`MaStRDownload`][open_mastr.soap_api.download.MaStRDownload] once and you get asked for a user name and a token. The
    information you insert will be used to create the credentials file.

    It is also possible to create the credentials file by hand, using this format:

    ```
        [MaStR]
        user = SOM123456789012
        token = msöiöo8u2o29933n31733m§=§1n33§304n... # optional, 540 characters
    ```

    The `token` should be written in one line, without line breaks.

    The credentials file needs to be stored at: `$HOME/.open-MaStR/config/credentials.cfg`

2. **Credentials file + keyring:** 
    The user is stored in the credentials file, while the token is stored encrypted in the [keyring](https://pypi.org/project/keyring/).

    Read in the documentation of the keyring library how to store your token in the
    keyring.

3. **Don't store:** 
    Just use the password for one query and forget it

    The latter option is only available when using :class:`open_mastr.soap_api.download.MaStRAPI`.
    Instantiate with

    ```python
    MaStRAPI(user='USERNAME', key='TOKEN')
    ```

    to provide user and token in a script and use these
    credentials in subsequent queries.