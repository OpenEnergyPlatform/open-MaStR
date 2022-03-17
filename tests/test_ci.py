from dummy_ci import dummy_ci_add


def test_ci_1():
    assert 6 == dummy_ci_add(4, 2)


def test_ci_2():
    assert 'Hello world' == dummy_ci_add('Hello', ' world')
