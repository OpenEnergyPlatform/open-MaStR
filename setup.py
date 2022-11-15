from setuptools import setup, find_packages
from os import path
import os

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="open_mastr",
    packages=[
        "open_mastr",
        "open_mastr.soap_api",
        "open_mastr.soap_api.metadata",
        "open_mastr.utils",
        "open_mastr.utils.config",
        "open_mastr.xml_download",
    ],
    version="0.12.1",
    description="A package that provides an interface for downloading and"
    " processing the data of the Marktstammdatenregister (MaStR)",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/OpenEnergyPlatform/open-MaStR",
    download_url="https://github.com/OpenEnergyPlatform/open-MaStR/archive"
    "/refs/tags/v0.12.1.tar.gz",
    author="Open Energy Family",
    author_email="datenzentrum@rl-institut.de",
    maintainer="Ludwig Hülk",
    maintainer_email="datenzentrum@rl-institut.de",
    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8, <4",  # 3.8 is needed for pandas 1.4
    install_requires=[
        "pandas>=1.4",  # pandas 1.4 is needed for pd.read_xml
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
            "xmltodict",
            "pre-commit",
        ]
    },
    package_data={
        "open_mastr": [
            os.path.join("utils", "config", "*.yml"),
            os.path.join("soap_api", "metadata", "LICENSE"),
        ]
    },
    project_urls={
        "Documentation": "https://open-mastr.readthedocs.io/",
        "Changelog": "https://open-mastr.readthedocs.io/en/latest/changelog.html",
        "Issue Tracker": "https://github.com/OpenEnergyPlatform/open-MaStR/issues",
        "Source": "https://github.com/OpenEnergyPlatform/open-MaStR",
    },
)
