import datetime
import math
import os
import shutil
import time
from datetime import datetime as dt
from importlib.metadata import PackageNotFoundError, version
from zipfile import ZipFile
from pathlib import Path
import urllib.request
import re
from datetime import datetime

import numpy as np
import requests
from tqdm import tqdm

# setup logger
from open_mastr.utils.config import setup_logger
from open_mastr.utils.constants import BULK_INCLUDE_TABLES_MAP, BULK_DATA
from open_mastr.utils import unzip_http

try:
    USER_AGENT = (
        f"open-mastr/{version('open-mastr')} python-requests/{version('requests')}"
    )
except PackageNotFoundError:
    USER_AGENT = "open-mastr"
log = setup_logger()


def gen_version(
    when: time.struct_time = time.localtime(), use_version: str = "current"
) -> str:
    """
    Generates the current version.

    The version number is determined according to a fixed release cycle,
    which is by convention in sync with the changes to other german regulatory
    frameworks of the energy such as GeLI Gas and GPKE.

    The release schedule is twice per year on 1st of April and October.
    The version number is determined by the year of release and the running
    number of the release, i.e. the release on April 1st is release 1,
    while the release in October is release 2.

    Further, the release happens during the day, so on the day of the
    changeover, the exported data will still be in the old version/format.

    see <https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Release-Termine.pdf>

    Examples:
    2024-01-01 = version 23.2
    2024-04-01 = version 23.2
    2024-04-02 = version 24.1
    2024-09-30 = version 24.1
    2024-10-01 = version 24.1
    2024-10-02 = version 24.2
    2024-31-12 = version 24.2
    """

    year = when.tm_year
    release = 1

    if when.tm_mon < 4 or (when.tm_mon == 4 and when.tm_mday == 1):
        year = year - 1
        release = 2
    elif when.tm_mon > 10 or (when.tm_mon == 10 and when.tm_mday > 1):
        release = 2

    # Change to MaStR version number that was used before
    # For example: 24.1 -> 23.2
    if use_version == "before":
        if release == 1:
            year = year - 1
            release = 2
        else:
            release = 1
    # Change to MaStR version number that was used afterwards
    # For example: 24.1 -> 24.2
    elif use_version == "after":
        if release == 2:
            year = year + 1
            release = 1
        else:
            release = 2

    # only the last two digits of the year are used
    year = str(year)[-2:]
    return f"{year}.{release}"


def gen_url(
    when: time.struct_time = time.localtime(), use_version="current", use_stichtag=False
) -> str:
    """Generates the download URL for the specified date.

    Note that not all dates are archived on the website.
    Normally only today is available, the export is usually made
    between 02:00 and 04:00, which means before 04:00 the current data may not
    yet be available and the download could fail.

    Note also that this function will not be able to generate URLs for dates
    before 2024 because a different URL scheme was used then which had some random
    data embedded in the name to make it harder to automate downloads.


    Args:
        when (time.struct_time, optional): Time object used to generate url. Defaults to time.localtime().
        use_version (str, optional): One of "current", "before", "after". "current" will generate the url
        for the expected MaStR version. "before" will generate the url for the previous MaStR version.
        "after" will generate the url for the subsequent MaStR version.

        "current": Gesamtdatenexport_20250403_25.1.zip
        "before": Gesamtdatenexport_20250403_24.2.zip
        "after": Gesamtdatenexport_20250403_25.2.zip

        Defaults to "current".
    """
    version = gen_version(when, use_version)
    date = time.strftime("%Y%m%d", when)

    if use_stichtag:
        url_str = f"https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_{date}_{version}.zip"
    else:
        url_str = f"https://download.marktstammdatenregister.de/Gesamtdatenexport_{date}_{version}.zip"
    return url_str


def download_xml_Mastr(
    save_path: str,
    bulk_date_string: str,
    bulk_data_list: list,
    xml_folder_path: str,
    url: str = None,
) -> None:
    """Downloads the zipped MaStR.

    Parameters
    -----------
    save_path: str
        Full file path where the downloaded MaStR zip file will be saved.
    bulk_date_string: str
        Date for which the file should be downloaded.
    bulk_data_list: list
        List of tables/technologis to be downloaded.
    xml_folder_path: str
        Path where the downloaded MaStR zip file will be saved.
    url: str, optional
        Custom download URL. If None, generates URL based on bulk_date_string.
    """

    log.info("Starting the Download from marktstammdatenregister.de.")

    # Helper function to convert date string to time.struct_time
    def _parse_date_string(date_str):
        """Convert YYYYMMDD string to time.struct_time object."""
        try:
            # Use datetime.strptime for robust date parsing
            parsed_date = dt.strptime(date_str, "%Y%m%d")
            # Convert to time.struct_time using timetuple()
            return parsed_date.timetuple()
        except (ValueError, IndexError) as e:
            log.warning(f"Invalid date format '{date_str}': {e}. Using current date.")
            return time.localtime()

    # Parse the date string to time.struct_time (needed for both cases)
    url_time = _parse_date_string(bulk_date_string)

    # Determine the URL to use
    if url is None:
        # Generate URL from date string if no custom URL provided
        url = gen_url(url_time)
    # else: custom URL is already provided, use it as-is

    time_a = time.perf_counter()
    r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        log.warning(
            "Download file was not found. Assuming that the new file was not published yet and retrying with yesterday."
        )
        now = time.localtime(
            time.mktime(url_time) - (24 * 60 * 60)
        )  # subtract 1 day from the date
        url = gen_url(now)
        r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        url = gen_url(url_time, use_version="before")  # Use lower MaStR Version
        log.warning(
            f"Download file was not found. Assuming that the version of MaStR has changed and retrying with download link: {url}"
        )
        r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        url = gen_url(url_time, use_version="after")  # Use higher MaStR Version
        log.warning(
            f"Download file was not found. Assuming that the version of MaStR has changed and retrying with download link: {url}"
        )
        r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})

    if r.status_code == 404:
        url = gen_url(
            url_time, use_stichtag=True
        )  # Use different url-structure for older downloads
        log.warning(
            f"Download file was not found. Assuming that the link structure of MaStR has changed and retrying with download link: {url}"
        )
        r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        log.error("Could not download file: download URL not found")
        return

    if bulk_data_list == BULK_DATA:
        full_download_without_unzip_http(save_path, r, bulk_data_list)
    else:
        try:
            partial_download_with_unzip_http(save_path, url, bulk_data_list)
        except Exception as e:
            log.warning(f"Partial download failed, fallback to full download: {e}")
            full_download_without_unzip_http(save_path, r, bulk_data_list)

    time_b = time.perf_counter()
    log.info(
        f"Download is finished. It took {int(np.around(time_b - time_a))} seconds."
    )
    log.info(f"MaStR was successfully downloaded to {xml_folder_path}.")


def check_download_completeness(
    save_path: str, bulk_data_list: list
) -> tuple[list, bool]:
    """Checks if an existing download contains the xml-files corresponding to the bulk_data_list."""
    with ZipFile(save_path, "r") as zip_ref:
        existing_files = [
            zip_name.lower().split("_")[0].split(".")[0]
            for zip_name in zip_ref.namelist()
        ]

    missing_data_set = set()
    for bulk_data_name in bulk_data_list:
        for bulk_file_name in BULK_INCLUDE_TABLES_MAP[bulk_data_name]:
            if bulk_file_name not in existing_files:
                missing_data_set.add(bulk_data_name)

    is_katalogwerte_existing = False
    if "katalogwerte" in existing_files:
        is_katalogwerte_existing = True
    return list(missing_data_set), is_katalogwerte_existing


def delete_xml_files_not_from_given_date(
    save_path: str,
    xml_folder_path: str,
) -> None:
    """
    Delete xml files that are not corresponding to the given date.
    Assumes that the xml folder only contains one zipfile.

    Parameters
    ----------
    save_path: str
        Full file path where the downloaded MaStR zip file will be saved.
    xml_folder_path: str
        Path where the downloaded MaStR zip file will be saved.
    """
    if os.path.exists(save_path):
        return
    else:
        shutil.rmtree(xml_folder_path)
        os.makedirs(xml_folder_path)


def partial_download_with_unzip_http(save_path: str, url: str, bulk_data_list: list):
    """

    Parameters
    ----------
    save_path: str
        Full file path where the downloaded MaStR zip file will be saved.
    url: str
        URL path to bulk file.
    bulk_data_list: list
        List of tables/technologis to be downloaded.

    Returns
    -------
    None
    """
    is_katalogwerte_existing = False
    if os.path.exists(save_path):
        bulk_data_list, is_katalogwerte_existing = check_download_completeness(
            save_path, bulk_data_list
        )
        if bool(bulk_data_list):
            log.info(
                f"MaStR file already present but missing the following data: {bulk_data_list}"
            )
        else:
            log.info(f"MaStR file already present: {save_path}")
            return None

    remote_zip_file = unzip_http.RemoteZipFile(url)
    remote_zip_names = [
        remote_zip_name.lower().split("_")[0].split(".")[0]
        for remote_zip_name in remote_zip_file.namelist()
    ]

    remote_index_list = []
    download_files_list = []
    for bulk_data_name in bulk_data_list:
        # Example: ['wind','solar']
        for bulk_file_name in BULK_INCLUDE_TABLES_MAP[bulk_data_name]:
            # Example: From "wind" we get ["anlageneegwind", "einheitenwind"], and  from "solar" we get ["anlageneegsolar", "einheitensolar"]
            # and we have to find the corresponding index in the remote_zip_file list in order to fetch the correct file
            remote_index_list = [
                remote_index
                for remote_index, remote_zip_name in enumerate(remote_zip_names)
                if remote_zip_name == bulk_file_name
            ]
            # for remote_index in tqdm(remote_index_list):
            for remote_index in remote_index_list:
                # Example: remote_zip_file.namelist()[remote_index] corresponds to e.g. 'AnlagenEegSolar_1.xml'
                download_files_list.append(remote_zip_file.namelist()[remote_index])

    for zipfile_name in tqdm(download_files_list, unit=" file"):
        remote_zip_file.extractzip(zipfile_name, path=Path(save_path))

    if not is_katalogwerte_existing:
        remote_zip_file.extractzip("Katalogwerte.xml", path=Path(save_path))


def full_download_without_unzip_http(
    save_path: str,
    r: requests.models.Response,
    bulk_data_list: list,
) -> None:
    """

    Parameters
    ----------
    save_path: str
        Full file path where the downloaded MaStR zip file will be saved.
    r: requests.models.Response
        Response from making a request to MaStR.
    bulk_data_list: list
        List of tables/technologis to be downloaded.

    Returns
    -------
    None
    """
    if os.path.exists(save_path):
        bulk_data_list, is_katalogwerte_existing = check_download_completeness(
            save_path, bulk_data_list
        )
        if bool(bulk_data_list):
            print(
                f"MaStR file already present but missing the following data: {bulk_data_list}"
            )
        else:
            print(f"MaStR file already present: {save_path}")
            return None

    warning_message = (
        "Warning: The servers from MaStR restrict the download speed."
        " You may want to download it another time."
    )
    # TODO: Explain this number
    total_length = int(23000)
    with (
        open(save_path, "wb") as zfile,
        tqdm(desc=save_path, total=total_length, unit="") as bar,
    ):
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            # chunk size of 1024 * 1024 needs 9min 11 sek = 551sek
            # chunk size of 1024 needs 9min 11 sek as well
            if chunk:
                zfile.write(chunk)
                zfile.flush()
            bar.update()
            # if the rate falls below 100 kB/s -> prompt warning
            if bar.format_dict["rate"] and bar.format_dict["rate"] < 2:
                bar.set_postfix_str(s=warning_message)
            else:
                # remove warning
                bar.set_postfix_str(s="")


def get_available_download_links(
    url="https://www.marktstammdatenregister.de/MaStR/Datendownload",
):
    """
    Fetch all available download links from the MaStR website.

    This function retrieves all available Gesamtdatenexport files from the MaStR
    download page, including both current and historical exports.

    Parameters
    ----------
    url : str, optional
        The URL of the MaStR download page. Defaults to the official download page.

    Returns
    -------
    list of dict
        A list of dictionaries containing information about available downloads.
        Each dictionary contains:
        - 'url': The download URL
        - 'date': The date of the export (YYYYMMDD format)
        - 'version': The MaStR version (e.g., '24.1', '24.2')
        - 'type': 'current' for current exports, 'stichtag' for historical exports

    Examples
    --------
    >>> links = get_available_download_links()
    >>> for link in links[:3]:
    ...     print(f"Date: {link['date']}, Version: {link['version']}, Type: {link['type']}")
    Date: 20250103, Version: 24.2, Type: current
    Date: 20241231, Version: 24.2, Type: current
    Date: 20241230, Version: 24.2, Type: current
    """
    log.info("Fetching available download links from MaStR website...")

    headers = {"User-Agent": USER_AGENT}
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            html = response.read().decode("utf-8")
    except Exception as e:
        log.error(f"Failed to fetch download page: {e}")
        return []

    # Pattern for current exports
    pattern_current = re.compile(
        r"https://download\.marktstammdatenregister\.de/Gesamtdatenexport_([0-9]{8})_([0-9]{2}\.[0-9])\.zip"
    )
    # Pattern for historical exports (Stichtag)
    pattern_stichtag = re.compile(
        r"https://download\.marktstammdatenregister\.de/Stichtag/Gesamtdatenexport_([0-9]{8})_([0-9]{2}\.[0-9])\.zip"
    )

    # Find all current export links
    current_matches = pattern_current.findall(html)
    current_links = [
        {
            "url": f"https://download.marktstammdatenregister.de/Gesamtdatenexport_{date}_{version}.zip",
            "date": date,
            "version": version,
            "type": "current",
        }
        for date, version in current_matches
    ]

    # Find all historical export links
    stichtag_matches = pattern_stichtag.findall(html)
    stichtag_links = [
        {
            "url": f"https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_{date}_{version}.zip",
            "date": date,
            "version": version,
            "type": "stichtag",
        }
        for date, version in stichtag_matches
    ]

    # Combine and sort by date (newest first)
    all_links = current_links + stichtag_links
    all_links.sort(key=lambda x: x["date"], reverse=True)

    log.info(f"Found {len(all_links)} available download links")
    return all_links


def list_available_downloads():
    """
    Display available downloads in a user-friendly format.

    Returns
    -------
    list of dict
        List of available downloads with formatted dates and versions.
    """
    links = get_available_download_links()

    if not links:
        print("No download links found. Please check your internet connection.")
        return []

    print("\n" + "=" * 80)
    print("AVAILABLE MAStR DOWNLOADS")
    print("=" * 80)
    print(f"{'#':<4} {'Date':<12} {'Version':<10} {'Type':<12} {'URL'}")
    print("-" * 80)

    for i, link in enumerate(links, 1):
        # Format date for better readability
        date_formatted = f"{link['date'][:4]}-{link['date'][4:6]}-{link['date'][6:]}"
        print(
            f"{i:<4} {date_formatted:<12} {link['version']:<10} {link['type']:<12} {link['url']}"
        )

    print("=" * 80)
    print(f"Total: {len(links)} downloads available")
    print("=" * 80)

    return links


def select_download_date():
    """
    Interactive function to let the user select a download date.

    Prompts the user to choose from available downloads or enter a custom date.

    Returns
    -------
    tuple
        (date_string, url) where date_string is in YYYYMMDD format and url is the download URL
        Returns (None, None) if user cancels or no valid selection is made
    """
    links = list_available_downloads()

    if not links:
        return None, None

    print("\nOptions:")
    print("1. Select from the list above (enter the number)")
    print("2. Cancel")

    while True:
        choice = input("\nPlease enter your choice (1-2): ").strip()

        if choice == "1":
            # Select from list
            while True:
                try:
                    index = int(input(f"Enter a number (1-{len(links)}): ").strip())
                    if 1 <= index <= len(links):
                        selected = links[index - 1]
                        print(
                            f"\nSelected: {selected['date']} (Version {selected['version']}, Type: {selected['type']})"
                        )
                        return selected["date"], selected["url"]
                    else:
                        print(f"Please enter a number between 1 and {len(links)}")
                except ValueError:
                    print("Please enter a valid number")

        elif choice == "2":
            print("Download selection cancelled.")
            return None, None

        else:
            print("Invalid choice. Please enter 1, or 2.")


def download_documentation(
    save_path: str, xml_folder_path: str
) -> None:
    """Downloads the zipped MaStR.

    Parameters
    -----------
    save_path: str
        Full file path where the downloaded MaStR zip file will be saved.
    xml_folder_path: str
        Path where the downloaded MaStR zip file will be saved.
    """
    log.info("Starting the MaStR documentation download from marktstammdatenregister.de.")
    url = "https://www.marktstammdatenregister.de/MaStRHilfe/files/gesamtdatenexport/Dokumentation%20MaStR%20Gesamtdatenexport.zip"

    time_a = time.perf_counter()
    r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})

    r.raise_for_status()

    chunk_size = 1024 * 1024
    content_length = r.headers.get("Content-Length")
    expected_steps = math.ceil(content_length / chunk_size)
    with (
        open(save_path, "wb") as zfile,
        tqdm(desc=save_path, total=expected_steps) as bar,
    ):
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                zfile.write(chunk)
                zfile.flush()
            bar.update()

    time_b = time.perf_counter()
    log.info(
        f"MaStR documentation download is finished. It took {round(time_b - time_a)} seconds."
    )
    log.info(f"MaStR was successfully downloaded to {xml_folder_path}.")
