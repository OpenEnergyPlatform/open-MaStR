from io import BytesIO
import re
from urllib.request import urlopen
from zipfile import ZipFile
import xmltodict
from collections import OrderedDict


class DataDescription(object):
    """
    Fundamental data for creating a description of open-MaStR data

    Extract documentation data from MaStR webservice XML file.

    .. code:: python

       functions_data_docs = DataDescription().functions_data_documentation()
    """

    def __init__(self, xml=None):
        """

        Parameters
        ----------
        xml: str or path-like, optional
            Path of local mastrbasetypes.xsd file. If not provided, file will
            be downloaded from marktstammdatenregister.de
        """

        # Read XML file
        if xml:
            with open(xml, "r") as fh:
                self.xml = fh.read()
        else:
            # If no XML file is given, the file is read from an URL
            zipurl = (
                "https://www.marktstammdatenregister.de/MaStRHilfe/files/"
                "webdienst/Dienstbeschreibung_1_2_39_Produktion.zip"
            )

            with urlopen(zipurl) as zipresp:
                with ZipFile(BytesIO(zipresp.read())) as zfile:
                    self.xml = zfile.read("xsd/mastrbasetypes.xsd")

        # Parse XML and extract relevant data
        parsed = xmltodict.parse(self.xml, process_namespaces=False)
        self.complex_types = parsed["schema"]["complexType"]
        self.simple_types = parsed["schema"]["simpleType"]

        # Prepare parsed data for documentational purposes
        abstract_types, parameters, responses, types = self._filter_type_descriptions()
        self.function_parameters = parameters
        self.function_responses = responses
        self.types = {**abstract_types, **types}
        self.simple_types_prepared = self.prepare_simple_type()

    def _filter_type_descriptions(self):
        """
        Reorganize complex data types
        """

        functions = []
        parameters = {}
        responses = {}
        abstract_types = {}
        types = {}

        for item in self.complex_types:

            # Filter out abstract types
            # Those are use to inherit columns from
            if "Basis" in item["@name"]:
                if "sequence" in item.keys():
                    abstract_types[item["@name"]] = item["sequence"]
                elif "complexContent" in item.keys():
                    abstract_types[item["@name"]] = item["complexContent"]
                else:
                    raise ValueError("Ohh...")
            else:
                # Filter all functions
                if item["@name"].startswith(
                    ("Get", "Set", "Erneute", "Verschiebe", "Delete")
                ):
                    functions.append(item)

                    # Further split the list of functions into paramters and responses
                    if item["@name"].endswith("Parameter"):
                        if "complexContent" in item.keys():
                            parameters[item["@name"]] = item["complexContent"][
                                "extension"
                            ]
                        else:
                            parameters[item["@name"]] = item
                    elif item["@name"].endswith("Antwort"):
                        # responses.append(item)
                        responses[item["@name"]] = item["complexContent"]["extension"]
                    else:
                        raise ValueError("Should be Parameter or Antwort!")
                else:
                    types[item["@name"]] = item

        return abstract_types, parameters, responses, types

    def prepare_simple_type(self):
        """
        Reformatting and filtering of simple MaStR type documentation

        Returns
        -------
        dict
            Data type and possible values for each simple MaStR type keyed by type name
        """

        simple_types_doc = {}

        for simple_type in self.simple_types:
            if "enumeration" in simple_type["restriction"]:
                possible_values = [
                    _["@value"] for _ in simple_type["restriction"]["enumeration"]
                ]
            else:
                possible_values = []
            simple_types_doc[simple_type["@name"]] = {
                "type": simple_type["restriction"]["@base"],
                "values": possible_values,
            }
        return simple_types_doc

    def functions_data_documentation(self):
        """
        Documentation for the data returned by MaStR webservice functions

        Returns
        -------
        dict
            Documentation of each column/field returned by a function keyed by function names
        """
        function_docs = {}
        for name, fcn in self.function_responses.items():
            fcn_name = name.replace("Antwort", "")
            if fcn["sequence"]:
                # Slice data depending on what is available
                if isinstance(fcn["sequence"]["element"], list):
                    fcn_data = fcn["sequence"]["element"]
                elif isinstance(fcn["sequence"]["element"], (dict, OrderedDict)):
                    if "annotation" in fcn["sequence"]["element"]:
                        fcn_data = [fcn["sequence"]["element"]]
                    else:
                        fcn_data = self.types[
                            fcn["sequence"]["element"]["@type"].split(":")[1]
                        ]["sequence"]["element"]
                else:
                    print(type(fcn["sequence"]))
                    print(fcn["sequence"])
                    raise ValueError

                # Add data for inherited columns from base types
                if "@base" in fcn:
                    if not fcn["@base"] == "mastr:AntwortBasis":
                        fcn_data = _collect_columns_of_base_type(
                            self.types, fcn["@base"].split(":")[1], fcn_data
                        )
                function_docs[fcn_name] = {}
                for column in fcn_data:
                    # Replace MaStR internal types with more general ones
                    if column["@type"].startswith("mastr:"):
                        try:
                            column_type = self.simple_types_prepared[
                                column["@type"].split(":")[1]
                            ]["type"]
                        except KeyError:
                            column_type = column["@type"]
                    else:
                        column_type = column["@type"]

                    if "annotation" in column.keys():
                        description = column["annotation"]["documentation"].get(
                            "#text", None
                        )
                        if description:
                            description = re.sub(
                                " +", " ", description.replace("\n", "")
                            )
                        function_docs[fcn_name][column["@name"]] = {
                            "type": column_type,
                            "description": description,
                            "example": column["annotation"]["documentation"].get(
                                "m-ex", None
                            ),
                        }
                    else:
                        function_docs[fcn_name][column["@name"]] = {
                            "type": column_type,
                            # TODO: insert information from simple type here
                            "description": None,
                            "example": None,
                        }

        # Hack in a descrition for a column that gets created after download while flattening data
        function_docs["GetEinheitWind"]["HerstellerId"] = {
            "type": "str",
            "description": "Id des Herstellers der Einheit",
            "example": 923,
        }

        return function_docs


def _collect_columns_of_base_type(base_types, base_type_name, fcn_data):
    type_description = base_types[base_type_name]
    fcn_data += type_description["extension"]["sequence"]["element"]

    if "@base" in type_description["extension"]:
        if not type_description["extension"]["@base"] == "mastr:AntwortBasis":
            fcn_data = _collect_columns_of_base_type(
                base_types,
                type_description["extension"]["@base"].split(":")[1],
                fcn_data,
            )

    return fcn_data
