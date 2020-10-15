from open_mastr.soap_api.download import MaStRAPI


def test_soap_wrapper_connection():
    mastr_api = MaStRAPI(user="testuser", key="testpassword")
    lokale_uhrzeit = mastr_api.GetLokaleUhrzeit()

    assert lokale_uhrzeit['Ergebniscode'] == 'OK'