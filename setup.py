from setuptools import setup, find_packages
from os import path
import os

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="open_mastr",
    packages=[
        "open_mastr",
        "open_mastr.soap_api",
        "open_mastr.soap_api.config",
        "open_mastr.soap_api.metadata",
        "open_mastr.utils",
        "open_mastr.xml_download",
    ],
    version="0.11.1",
    description="The python package open_mastr provides an interface for "
                "accessing the data. It contains methods to download and "
                "parse the xml files (bulk) and the web service (API).",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OpenEnergyPlatform/open-MaStR",
    download_url="https://github.com/OpenEnergyPlatform/open-MaStR/archive"
                 "/refs/tags/v0.11.1.tar.gz",
    maintainer_email="datenzentrum@rl-institut.de",

    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.8, <4",
    install_requires=[
        "pandas>=1.4",
        "numpy",
        "sqlalchemy",
        "psycopg2-binary",
        "zeep",
        "tqdm",
        "requests",
        "keyring",
        "pynodo",
        "tqdm",
        "beautifulsoup4",
        "pyyaml",
        "xmltodict",
    ],
    extras_require={
        "dev": [
            "flake8",
            "pylint",
            "pytest",
            "pytest-dependency",
            "xmltodict",  # TODO: Only needed in metadata
        ]
    },
    package_data={
        "open_mastr": [
            os.path.join("soap_api", "config", "*.yml"),
            os.path.join("soap_api", "metadata", "LICENSE"),
        ]
    },
    project_urls={
        "Bug Reports": "https://github.com/OpenEnergyPlatform/open-MaStR/issues",
        "Source": "https://github.com/OpenEnergyPlatform/open-MaStR",
    },
)

#os.system("ulimit -n 1000000")
