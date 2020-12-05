from open_mastr.utils.credentials import get_mastr_user, get_mastr_token


def test_get_mastr_user_and_token():

    user = get_mastr_user()
    assert len(user) == 15

    token = get_mastr_token(user)
    assert len(token) == 540
