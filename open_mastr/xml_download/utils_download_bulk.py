import os
import shutil
import time
from importlib.metadata import PackageNotFoundError, version
from zipfile import BadZipfile, ZipFile

import numpy as np
import requests
from tqdm import tqdm

# setup logger
from open_mastr.utils.config import setup_logger

try:
    USER_AGENT = (
        f"open-mastr/{version('open-mastr')} python-requests/{version('requests')}"
    )
except PackageNotFoundError:
    USER_AGENT = "open-mastr"
log = setup_logger()


def gen_version(when: time.struct_time = time.localtime()) -> str:
    """
    Generates the current version.

    The version number is determined according to a fixed release cycle,
    which is by convention in sync with the changes to other german regulatory
    frameworks of the energysuch as GeLI Gas and GPKE.

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

    # only the last two digits of the year are used
    year = str(year)[-2:]

    return f"{year}.{release}"


def gen_url(when: time.struct_time = time.localtime()) -> str:
    """
    Generates the download URL for the specified date.

    Note that not all dates are archived on the website.
    Normally only today is available, the export is usually made
    between 02:00 and 04:00, which means before 04:00 the current data may not
    yet be available and the download could fail.

    Note also that this function will not be able to generate URLs for dates
    before 2024 because a different URL scheme was used then which had some random
    data embedded in the name to make it harder to automate downloads.
    """

    version = gen_version(when)
    date = time.strftime("%Y%m%d", when)

    return f"https://download.marktstammdatenregister.de/Gesamtdatenexport_{date}_{version}.zip"


def download_xml_Mastr(
    save_path: str, bulk_date_string: str, xml_folder_path: str
) -> None:
    """Downloads the zipped MaStR.

    Parameters
    -----------
    save_path: str
        The path where the downloaded MaStR zipped folder will be saved.
    """

    if os.path.exists(save_path):
        try:
            _ = ZipFile(save_path)
        except BadZipfile:
            log.info(f"Bad Zip file is deleted: {save_path}")
            os.remove(save_path)
        else:
            print("MaStR already downloaded.")
            return None

    if bulk_date_string != "today":
        raise OSError(
            "There exists no file for given date. MaStR can only be downloaded "
            "from the website if today's date is given."
        )
    shutil.rmtree(xml_folder_path, ignore_errors=True)
    os.makedirs(xml_folder_path, exist_ok=True)

    print_message = (
        "Download has started, this can take several minutes."
        "The download bar is only a rough estimate."
    )
    warning_message = (
        "Warning: The servers from MaStR restrict the download speed."
        " You may want to download it another time."
    )
    print(print_message)

    now = time.localtime()
    url = gen_url(now)

    time_a = time.perf_counter()
    r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        log.warning(
            "Download file was not found. Assuming that the new file was not published yet and retrying with yesterday."
        )
        now = time.localtime(
            time.mktime(now) - (24 * 60 * 60)
        )  # subtract 1 day from the date
        url = gen_url(now)
        r = requests.get(url, stream=True, headers={"User-Agent": USER_AGENT})
    if r.status_code == 404:
        log.error("Could not download file: download URL not found")
        return

    total_length = int(18000 * 1024 * 1024)
    with (
        open(save_path, "wb") as zfile,
        tqdm(desc=save_path, total=(total_length / 1024 / 1024), unit="") as bar,
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
    time_b = time.perf_counter()
    print(f"Download is finished. It took {int(np.around(time_b - time_a))} seconds.")
    print(f"MaStR was successfully downloaded to {xml_folder_path}.")
