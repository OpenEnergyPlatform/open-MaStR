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
    <a href="https://download.marktstammdatenregister.de/Gesamtdatenexport_20260103_25.2.zip"></a>
    <a href="/MaStRHilfe/files/gesamtdatenexport/Dokumentation%20MaStR%20Gesamtdatenexport.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20260101_25.2.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Dokumentation%20MaStR%20Gesamtdatenexport%2001-01-2026.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20251001_25.2.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Dokumentation%20MaStR%20Gesamtdatenexport%2001-10-2025.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20251001_25.1.zip"></a>
    <a href="https://download.marktstammdatenregister.de/Stichtag/Dokumentation%20MaStR%20Gesamtdatenexport%2001-10-2025.zip"></a>
</body>
</html>
"""

# Sample download links for mocking
SAMPLE_LINKS = [
    {
        "url": "https://download.marktstammdatenregister.de/Gesamtdatenexport_20260103_25.2.zip",
        "docs_url": "https://www.marktstammdatenregister.de/MaStRHilfe/files/gesamtdatenexport/Dokumentation%20MaStR%20Gesamtdatenexport.zip",
        "date": "20260103",
        "version": "25.2",
        "type": "current",
    },
    {
        "url": "https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20260101_25.2.zip",
        "docs_url": "https://download.marktstammdatenregister.de/Stichtag/Dokumentation%20MaStR%20Gesamtdatenexport%2001-01-2026.zip",
        "date": "20260101",
        "version": "25.2",
        "type": "stichtag",
    },
    {
        "url": "https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20251001_25.2.zip",
        "docs_url": "https://download.marktstammdatenregister.de/Stichtag/Dokumentation%20MaStR%20Gesamtdatenexport%2001-10-2025.zip",
        "date": "20251001",
        "version": "25.2",
        "type": "stichtag",
    },
    {
        "url": "https://download.marktstammdatenregister.de/Stichtag/Gesamtdatenexport_20251001_25.1.zip",
        "docs_url": "https://download.marktstammdatenregister.de/Stichtag/Dokumentation%20MaStR%20Gesamtdatenexport%2001-10-2025.zip",
        "date": "20251001",
        "version": "25.1",
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
    assert links == SAMPLE_LINKS


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
        "#    Date         Version    Type         XML URL"
        "                                                                                    Docs URL"
    )
    mock_print.assert_any_call("Total: 4 downloads available")


@patch("open_mastr.xml_download.utils_download_bulk.list_available_downloads")
def test_select_download_date_valid_selection(mock_list_downloads):
    """Test interactive date selection with valid user input."""
    mock_list_downloads.return_value = SAMPLE_LINKS

    # Simulate user choosing option 1, then selecting the 2nd item
    with patch("builtins.input", side_effect=["1", "2"]):
        link = select_download_date()
        assert link == SAMPLE_LINKS[1]


@patch("open_mastr.xml_download.utils_download_bulk.list_available_downloads")
def test_select_download_date_cancel(mock_list_downloads):
    """Test interactive date selection when the user cancels."""
    mock_list_downloads.return_value = SAMPLE_LINKS

    # Simulate user choosing option 2 (Cancel)
    with patch("builtins.input", side_effect=["2"]):
        link = select_download_date()
        assert link is None


@patch("open_mastr.mastr.Mastr.generate_data_model")
@patch("open_mastr.mastr.write_mastr_xml_to_database")
@patch("open_mastr.mastr.select_download_date")
@patch("open_mastr.mastr.download_xml_Mastr")
def test_mastr_download_interactive(
    mock_download,
    mock_select_date,
    mock_write_db,
    mock_generate_data_model,
    mastr: Mastr,
):
    """Test the main download method with interactive selection."""
    link = SAMPLE_LINKS[0]
    mock_select_date.return_value = link
    mastr.download(select_date_interactively=True)

    # Assert that select_download_date was called
    mock_select_date.assert_called_once()

    # Assert that generate_data_model was called with the correct URL
    mock_generate_data_model.assert_called_once()
    _, kwargs = mock_generate_data_model.call_args
    assert kwargs["url"] == link["docs_url"]

    # Assert that download_xml_Mastr was called with the correct URL
    mock_download.assert_called_once()
    args, _ = mock_download.call_args
    assert args[4] == link["url"]
    assert args[1] == link["date"]


@patch("open_mastr.mastr.select_download_date")
@patch("open_mastr.mastr.download_xml_Mastr")
def test_mastr_download_interactive_cancel(mock_download, mock_select_date, mastr: Mastr):
    """Test the main download method when interactive selection is cancelled."""
    mock_select_date.return_value = None
    mastr.download(select_date_interactively=True)

    # Assert that select_download_date was called
    mock_select_date.assert_called_once()

    # Assert that download_xml_Mastr was NOT called
    mock_download.assert_not_called()


@patch("open_mastr.mastr.list_available_downloads")
def test_mastr_browse_available_downloads(mock_list_downloads, mastr: Mastr):
    """Test the browse_available_downloads method."""
    mock_list_downloads.return_value = SAMPLE_LINKS
    result = mastr.browse_available_downloads()

    mock_list_downloads.assert_called_once()
    assert result == SAMPLE_LINKS
