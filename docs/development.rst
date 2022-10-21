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

If structure of functions (new added/deleted or names changed) changes, code reference needs
to be updated. 

.. code-block:: bash

   sphinx-apidoc --no-toc -o docs/reference open_mastr open_mastr/postprocessing

If the SOAP API of MaStR changes, data table metadata documentation needs to be updated as well. From within
`open-MaStR/docs` execute

.. code-block:: bash

   python -c "from open_mastr.utils.docs import generate_data_docs; generate_data_docs()"


to recreate the files.


Code Formatting
=================

We use `Black <https://github.com/psf/black>`_ as Python formatter. In order to simplify and guarantee the proper usage,
Black is added as a pre-commit hook. When you try to commit some changes, the changed files will eventually be formatted
and your commit fails. After adding the new changes to your commit, you should have a clean formatted code and
a successful commit.

The first time after installing the package in development mode, you have to install the pre-commit hooks.

.. code-block:: bash

    pre-commit install


Testing
=======

Make sure you have test requirements installed (in addition to the package itself).

.. code-block:: bash

   pip install -e .[dev]

Some tests query data from `MaStR <https://www.marktstammdatenregister.de>`_. Therefore, user name and the token must
be provided by credentials file `credentials.cfg` (with token optionally stored in keyring).
Further explanation :ref:`here <MaStR account and credentials>`.

`PyTest <https://docs.pytest.org/en/stable/index.html>`_ is used to run the test suite.

.. code-block:: bash

   pytest -vv

Updating open-mastr ORM due to wsdl API change
=================================

The Markstammdatenregister API is yearly updated on 1 April and 1 October. The updates often entail changes, additions or deletions of columns in the data base.
This changes have to be reflected in the code by adapting:

1. orm.py - ensuring a faultless download with `method='API'`
2. mastr_datapackage.json - updating the metadata for correct data description
3. docs\_data\_raw\ - updating tables for correct online documentation

You need to set up a new database with the adapted ORM structure locally to apply the changes after implementation. Your existing database won't reflect the changes.

The issue `#351 <https://github.com/OpenEnergyPlatform/open-MaStR/issues/351>`_ in the open-mastr GitHub repo can serve as a process blueprint for future updates.


Validating metadata documentation
=================================

From with a directory of data (default case: `~/.open-MaStR/data/<data-version>` execute

.. code-block:: bash

   frictionless validate datapackage.json --basepath .

for validating datapackage metadata with
`Frictionless data specifications
<https://framework.frictionlessdata.io/docs/guides/validation-guide#validating-package>`_.
At the moment, there complaints about the format.
