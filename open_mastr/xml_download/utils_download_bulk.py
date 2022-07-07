import requests
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import numpy as np


def get_url_from_Mastr_website() -> str:
    """Get the url of the latest MaStR file from markstammdatenregister.de.

    The file and the corresponding url are updated once per day.
    The url has a randomly generated string appended, so it has to be
    grabbed from the marktstammdatenregister.de homepage.
    For further details visit https://www.marktstammdatenregister.de/MaStR/Datendownload
    """

    html = requests.get("https://www.marktstammdatenregister.de/MaStR/Datendownload")
    soup = BeautifulSoup(html.text, "lxml")
    # find the download button element on the website
    element = soup.find_all("a", "btn btn-primary text-right")[0]
    # extract the url from the html element
    url = str(element).split('href="')[1].split('" title')[0]
    return url


def download_xml_Mastr(save_path: str) -> None:
    """Downloads the zipped MaStR.

    Parameters
    -----------
    save_path: str
        The path where the downloaded MaStR zipped folder will be saved.
    """
    print_message = (
        "Download has started, this can take several minutes."
        "The download bar is only a rough estimate."
    )
    warning_message = (
        "Warning: The servers from MaStR restrict the download speed."
        " You may want to download it another time."
    )
    print(print_message)
    url = get_url_from_Mastr_website()
    time_a = time.perf_counter()
    r = requests.get(url, stream=True)
    total_length = int(10000 * 1024 * 1024)
    with open(save_path, "wb") as zfile, tqdm(
        desc=save_path, total=(total_length / 1024 / 1024), unit="MB"
    ) as bar:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            # chunk size of 1024 * 1024 needs 9min 11 sek = 551sek
            # chunk size of 1024 needs 9min 11 sek as well
            if chunk:
                zfile.write(chunk)
                zfile.flush()
            bar.update()
            # if the rate falls below 100 kB/s -> prompt warning
            if bar.format_dict["rate"] and bar.format_dict["rate"] < 0.1:
                bar.set_postfix_str(s=warning_message)
            else:
                # remove warning
                bar.set_postfix_str(s="")
    time_b = time.perf_counter()
    print(f"Download is finished. It took {int(np.around(time_b - time_a))} seconds.")
