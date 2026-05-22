"""One-time script to create a small bulk download mockup for testing.

Downloads wind, biomass and hydro data from the MaStR bulk download, trims each
XML table to at most MAX_RECORDS records, and writes the result as
tests/data/Gesamtdatenexport_mockup.zip.

Run this whenever the mockup needs to be regenerated from real data, e.g. after
a MaStR format change:

    python scripts/create_bulk_download_mockup.py
"""

import io
import sys
import tempfile
import zipfile
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).parent.parent))
from open_mastr import Mastr
from open_mastr.utils.constants import BULK_INCLUDE_TABLES_MAP

MAX_RECORDS = 1000
TECHNOLOGIES = ["wind", "biomass", "hydro"]
OUTPUT_ZIP = (
    Path(__file__).parent.parent / "tests" / "data" / "Gesamtdatenexport_mockup.zip"
)

INCLUDED_TABLE_NAMES = {
    table for tech in TECHNOLOGIES for table in BULK_INCLUDE_TABLES_MAP[tech]
} | {"katalogwerte"}


def trim_xml(content: bytes, max_records: int) -> bytes:
    """Return UTF-16 XML bytes with at most max_records records."""
    tree = etree.parse(io.BytesIO(content))
    root = tree.getroot()
    children = list(root)
    for child in children[max_records:]:
        root.remove(child)
    buf = io.BytesIO()
    tree.write(buf, encoding="UTF-16", xml_declaration=True)
    return buf.getvalue()


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Downloading to {tmpdir} …")
        mastr = Mastr(output_dir=tmpdir)
        mastr.download(data=TECHNOLOGIES, bulk_cleansing=False)

        xml_dir = Path(tmpdir) / "data" / "xml_download"
        zips = sorted(xml_dir.glob("Gesamtdatenexport_*.zip"))
        if not zips:
            sys.exit(f"ERROR: no zip found in {xml_dir}")
        src_zip = zips[-1]
        print(f"Source zip: {src_zip}  ({src_zip.stat().st_size / 1024 / 1024:.1f} MB)")

        OUTPUT_ZIP.parent.mkdir(parents=True, exist_ok=True)

        with (
            zipfile.ZipFile(src_zip, "r") as src,
            zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as dst,
        ):
            for name in src.namelist():
                base = name.lower().split("_")[0].split(".")[0]
                if base not in INCLUDED_TABLE_NAMES:
                    continue
                content = src.read(name)
                is_katalogwerte = base == "katalogwerte"
                if not is_katalogwerte:
                    content = trim_xml(content, MAX_RECORDS)
                dst.writestr(name, content)
                print(f"  added {name} ({len(content) / 1024:.1f} KB)")

    size_kb = OUTPUT_ZIP.stat().st_size / 1024
    print(f"\nMockup written to {OUTPUT_ZIP}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
