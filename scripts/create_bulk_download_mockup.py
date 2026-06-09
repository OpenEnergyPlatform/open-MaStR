"""One-time script to create a small bulk download mockup for testing.

Downloads wind, biomass and hydro data from the MaStR bulk download, trims each
XML table to at most MAX_RECORDS records, and writes the result as
tests/data/Gesamtdatenexport_mockup.zip.

Run this whenever the mockup needs to be regenerated from real data, e.g. after
a MaStR format change:

    python scripts/create_bulk_download_mockup.py
"""

import copy
import io
import math
import os
import random
import sys
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path

from lxml import etree
from multiprocessing import cpu_count

sys.path.insert(0, str(Path(__file__).parent.parent))
from open_mastr import Mastr
from open_mastr.utils.constants import BULK_INCLUDE_TABLES_MAP

MAX_RECORDS = 100  # should be dividable by four
TECHNOLOGIES = [
    "wind",
    "solar",
    "biomass",
    "hydro",
    "gsgk",
    "combustion",
    "nuclear",
    "gas",
    "storage",
    "storage_units",
    "electricity_consumer",
    "location",
    "market",
    "grid",
    "balancing_area",
    "permit",
    "deleted_units",
    "deleted_market_actors",
    "retrofit_units",
]
OUTPUT_ZIP = (
    Path(__file__).parent.parent / "tests" / "data" / "Gesamtdatenexport_mockup.zip"
)

# Set to a directory path for debugging (directory is kept after the script finishes).
# Leave as None to use a temporary directory that is cleaned up by the OS once in a while.
DEBUG_DIR: str | None = "tmp/mastr-debug"

INCLUDED_TABLE_NAMES = {
    table for tech in TECHNOLOGIES for table in BULK_INCLUDE_TABLES_MAP[tech]
} | {"katalogwerte"}


def get_table_base(filename: str) -> tuple[str, bool]:
    """Return (base_name, is_numbered) for an XML filename.

    'AnlagenEegSpeicher_3.xml' → ('AnlagenEegSpeicher', True)
    'EinheitenWind.xml'        → ('EinheitenWind', False)
    """
    stem = filename.rsplit(".", 1)[0]
    parts = stem.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], True
    return stem, False


def build_xml_bytes(root_tag: str, records: list) -> bytes:
    """Serialize a list of lxml elements under root_tag as UTF-16 XML bytes."""
    new_root = etree.Element(root_tag)
    for rec in records:
        new_root.append(rec)
    buf = io.BytesIO()
    etree.ElementTree(new_root).write(buf, encoding="UTF-16", xml_declaration=True)
    return buf.getvalue()


def sample_records_streaming(
    zip_file: zipfile.ZipFile, fname: str, n: int
) -> tuple[str | None, list]:
    """Reservoir-sample n records from a zipped XML entry. This sampling is
    memory efficient, which is important since the XML files are large. See
    https://en.wikipedia.org/wiki/Reservoir_sampling

    Uses iterparse + elem.clear() so only the sampled records and one element
    at a time are held in memory — the full tree is never built.
    """
    reservoir: list = []
    count = 0
    root_tag: str | None = None
    depth = 0

    with zip_file.open(fname) as raw_f:
        for event, elem in etree.iterparse(raw_f, events=("start", "end")):
            if event == "start":
                depth += 1
                if depth == 1:
                    root_tag = elem.tag
            else:
                if depth == 2:
                    count += 1
                    rec = copy.deepcopy(elem)
                    if len(reservoir) < n:
                        reservoir.append(rec)
                    else:
                        j = random.randint(0, count - 1)
                        if j < n:
                            reservoir[j] = rec
                    elem.clear()
                depth -= 1

    return root_tag, reservoir


def main() -> None:
    recommended = min(cpu_count() - 1, 4)
    num_procs = input(
        f"Number of processes for parallelized XML parsing? "
        f"(Recommended max: {recommended}; Leave empty for no parallelization or enter number of processes: "
    )
    if num_procs.strip():
        os.environ["NUMBER_OF_PROCESSES"] = num_procs

    if DEBUG_DIR:
        Path(DEBUG_DIR).mkdir(parents=True, exist_ok=True)
        tmpdir = DEBUG_DIR
    else:
        tmpdir = tempfile.mkdtemp()

    print(f"Downloading to {tmpdir} …")
    print("-------IMPORTANT NOTE-------")
    print(
        "After the zipped xml files were downloaded, you can abort the writing to the database."
    )
    print(
        "You can then comment out the mastr.download() line in this script and rerun this script."
    )
    print(
        "This works as the script only depends on the xml files being downloaded - it does not use the sqlite database."
    )
    print("-------IMPORTANT NOTE-------")
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
        # categorise files by table base name
        table_groups: dict[str, list[str]] = defaultdict(list)
        katalogwerte_name: str | None = None
        for name in src.namelist():
            base, _ = get_table_base(name)
            if base.lower() == "katalogwerte":
                katalogwerte_name = name
            elif base.lower() in INCLUDED_TABLE_NAMES:
                table_groups[base].append(name)

        # add katalogwerte to output zip
        if katalogwerte_name:
            content = src.read(katalogwerte_name)
            dst.writestr(katalogwerte_name, content)
            print(f"  added {katalogwerte_name} ({len(content) / 1024:.1f} KB)")

        # sample records per file and write ≤4 files per table
        for base, files in table_groups.items():
            n_per_file = math.ceil(MAX_RECORDS / len(files))
            root_tag = None
            sampled = []
            for fname in files:
                file_root_tag, records = sample_records_streaming(
                    src, fname, n_per_file
                )
                if root_tag is None:
                    root_tag = file_root_tag
                sampled.extend(records)
            # make sure that there are really only MAX_RECORDS there
            sampled = sampled[:MAX_RECORDS]
            if len(sampled) != MAX_RECORDS:
                print(f"ALERT: Only {len(sampled)} records available for {base}")

            # Create one file in mock if only one file is available in zipped download
            if len(files) == 1:
                output_chunks = [(files[0], sampled)]
            # Else create exactly four files in mock
            else:
                a = int(MAX_RECORDS / 4)
                output_chunks = [
                    (f"{base}_1.xml", sampled[:a]),
                    (f"{base}_2.xml", sampled[a : 2 * a]),
                    (f"{base}_3.xml", sampled[2 * a : 3 * a]),
                    (f"{base}_4.xml", sampled[3 * a :]),
                ]

            for out_name, records in output_chunks:
                content = build_xml_bytes(root_tag, records)
                dst.writestr(out_name, content)
                print(f"  added {out_name} ({len(content) / 1024:.1f} KB)")

    size_kb = OUTPUT_ZIP.stat().st_size / 1024
    print(f"\nMockup written to {OUTPUT_ZIP}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
