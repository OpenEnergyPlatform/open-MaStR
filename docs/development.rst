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
