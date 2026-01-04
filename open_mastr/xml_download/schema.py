from pathlib import Path
import glob
from xml.etree import ElementTree

import xmlschema


def check_if_files_valid_under_schema(xsd_file, xml_files):
    schema = xmlschema.XMLSchema(xsd_file)
    for xml_file in xml_files:
        xml_resource = xmlschema.XMLResource(xml_file, lazy=True)
        errors = schema.iter_errors(xml_resource)
        error_count = 0
        for error in errors:
            error_count += 1
            breakpoint()
            print(" -", error)
        if error_count == 0:
            print(f"{xml_file}\tValid.")


def check_if_files_valid_under_schema_et(xsd_file, xml_files):
    schema = xmlschema.XMLSchema(xsd_file)
    for xml_file in xml_files:
        xt = ElementTree.parse(xml_file)
        errors = schema.iter_errors(xt)
        error_count = 0
        for error in errors:
            error_count += 1
            breakpoint()
            print(" -", error)
        if error_count == 0:
            print(f"{xml_file}\tValid.")



def main():
    xsd_root = Path("/home/gorgor/.open-MaStR/data/xml_download/Dokumentation MaStR Gesamtdatenexport/xsd")
    xml_root = Path("/home/gorgor/.open-MaStR/data/xml_download/Gesamtdatenexport_20251129")
    xsd_file = xsd_root / "EinheitenWind.xsd"
    xml_files = [xml_root / basename for basename in glob.glob("EinheitenWind*.xml", root_dir=xml_root)]
    xml_files =["/home/gorgor/.open-MaStR/data/xml_download/EinheitenWind_formatted.xml"] 
    print(xsd_file)
    print(xml_files)
    check_if_files_valid_under_schema_et(xsd_file=xsd_file, xml_files=xml_files)


if __name__ == "__main__":
    main()
