***********
Development
***********

For now, please read in the
`CONTRIBUTING <https://github.com/OpenEnergyPlatform/open-MaStR/blob/master/CONTRIBUTING.md>`_.

Build docs
==========

For checking that the documentation builds successfully, run sphinx locally.
Make sure you've installed docs requirements

.. code-block:: bash

   pip install -r docs/requirements.txt

The you can build the docs with

.. code-block:: bash

   sphinx-build -E -a docs docs/_build/


Testing
=======

Make sure you have test requirements installed.

.. code-block:: bash

   pip install -r tests/test_requirements.txt

Some tests query data from `MaStR <https://www.marktstammdatenregister.de>`_. Therefore, user name and the token must
be provided by credentials file `credentials.cfg` (with token optionally stored in keyring).
Further explanation :ref:`here <MaStR account>`.

`PyTest <https://docs.pytest.org/en/stable/index.html>`_ is used to run the test suite.

.. code-block:: bash

   pytest -vv

