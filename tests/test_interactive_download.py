import pytest
from unittest.mock import patch, MagicMock
from open_mastr.xml_download.utils_download_bulk import (
    get_available_download_links,
    list_available_downloads,
    select_download_date,
)
from open_mastr.mastr import Mastr

# Sample HTML content for mocking urlopen
SAMPLE_HTML = """
<html>
<body>
    <a href="https://download.marktstammdatenregister.de/Gesamtdatenexport_20250103_24.2.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.2.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20241130_24.1.zip"></a>
</body>
</html>
"""

# Sample download links for mocking
SAMPLE_LINKS = [
    {
        "url": "https://download.marktstammdatenregister.de/Gesamtdatenexport_20250103_24.2.zip",
        "date": "20250103",
        "version": "24.2",
        "type": "current",
    },
    {
        "url": "https://download.marktstammdatenregister.de/Gesamtdatenexport_20241231_24.2.zip",
        "date": "20241231",
        "version": "24.2",
        "type": "current",
    },
    {
        "url": "https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20241130_24.1.zip",
        "date": "20241130",
        "version": "24.1",
        "type": "stichtag",
    },
]


@patch("urllib.request.urlopen")
def test_get_available_download_links(mock_urlopen):
    """Test fetching and parsing of download links."""
    mock_response = MagicMock()
    mock_response.read.return_value = SAMPLE_HTML.encode("utf-8")
    mock_response.__enter__.return_value = mock_response
    mock_urlopen.return_value = mock_response

    links = get_available_download_links()

    assert len(links) == 3
    assert links[0]["date"] == "20250103"
    assert links[0]["version"] == "24.2"
    assert links[0]["type"] == "current"
    assert links[2]["date"] == "20241130"
    assert links[2]["version"] == "24.1"
    assert links[2]["type"] == "stichtag"


@patch("open_mastr.xml_download.utils_download_bulk.get_available_download_links")
@patch("builtins.print")
def test_list_available_downloads(mock_print, mock_get_links):
    """Test the formatted output of available downloads."""
    mock_get_links.return_value = SAMPLE_LINKS

    result = list_available_downloads()

    assert result == SAMPLE_LINKS
    # Check that print was called with the expected header
    mock_print.assert_any_call("=" * 80)
    mock_print.assert_any_call("AVAILABLE MAStR DOWNLOADS")
    mock_print.assert_any_call(
        f"{'#':<4} {'Date':<12} {'Version':<10} {'Type':<12} {'URL'}"
    )


@patch("open_mastr.xml_download.utils_download_bulk.list_available_downloads")
def test_select_download_date_valid_selection(mock_list_downloads):
    """Test interactive date selection with valid user input."""
    mock_list_downloads.return_value = SAMPLE_LINKS

    # Simulate user choosing option 1, then selecting the 2nd item
    with patch("builtins.input", side_effect=["1", "2"]):
        date, url = select_download_date()
        assert date == "20241231"
        assert url == SAMPLE_LINKS[1]["url"]


@patch("open_mastr.xml_download.utils_download_bulk.list_available_downloads")
def test_select_download_date_cancel(mock_list_downloads):
    """Test interactive date selection when the user cancels."""
    mock_list_downloads.return_value = SAMPLE_LINKS

    # Simulate user choosing option 2 (Cancel)
    with patch("builtins.input", side_effect=["2"]):
        date, url = select_download_date()
        assert date is None
        assert url is None


@patch("open_mastr.mastr.write_mastr_xml_to_database")
@patch("open_mastr.mastr.select_download_date")
@patch("open_mastr.mastr.download_xml_Mastr")
def test_mastr_download_interactive(mock_download, mock_select_date, mock_write_db):
    """Test the main download method with interactive selection."""
    mock_select_date.return_value = ("20241231", "http://example.com/file.zip")
    db = Mastr()
    db.download(select_date_interactively=True)

    # Assert that select_download_date was called
    mock_select_date.assert_called_once()

    # Assert that download_xml_Mastr was called with the correct URL
    mock_download.assert_called_once()
    args, kwargs = mock_download.call_args
    assert args[4] == "http://example.com/file.zip"
    assert args[1] == "20241231"  # date argument


@patch("open_mastr.mastr.select_download_date")
@patch("open_mastr.mastr.download_xml_Mastr")
def test_mastr_download_interactive_cancel(mock_download, mock_select_date):
    """Test the main download method when interactive selection is cancelled."""
    mock_select_date.return_value = (None, None)
    db = Mastr()
    db.download(select_date_interactively=True)

    # Assert that select_download_date was called
    mock_select_date.assert_called_once()

    # Assert that download_xml_Mastr was NOT called
    mock_download.assert_not_called()


@patch("open_mastr.xml_download.utils_download_bulk.list_available_downloads")
def test_mastr_browse_available_downloads(mock_list_downloads):
    """Test the browse_available_downloads method."""
    mock_list_downloads.return_value = SAMPLE_LINKS
    db = Mastr()
    result = db.browse_available_downloads()

    mock_list_downloads.assert_called_once()
    assert result == SAMPLE_LINKS
