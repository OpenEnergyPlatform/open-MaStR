import xmlschema
from xmlschema.validators import XsdComplexType, XsdSimpleType, XsdElement
from typing import Dict, List, Optional

# ----------------------------------------------
# 1. Mapping XSD builtin types → SQLAlchemy types
# ----------------------------------------------
XSD_TO_SQLA = {
    "string": "String",
    "integer": "Integer",
    "int": "Integer",
    "short": "Integer",
    "long": "BigInteger",
    "decimal": "Float",
    "float": "Float",
    "double": "Float",
    "boolean": "Boolean",
    "date": "Date",
    "dateTime": "DateTime",
    "time": "Time",
}


def map_xsd_type(xsd_type: XsdSimpleType) -> str:
    """Map XSD builtin type to SQLAlchemy column type."""
    if xsd_type.is_simple() and xsd_type.primitive_type:
        name = xsd_type.primitive_type.local_name
        return XSD_TO_SQLA.get(name, "String")  # default fallback
    return "String"


# ----------------------------------------------
# 2. Main model generation
# ----------------------------------------------
def generate_sqlalchemy_models(xsd_file: str) -> str:
    schema = xmlschema.XMLSchema(xsd_file)
    output = []

    output.append("from sqlalchemy import Column, Integer, String, Float, Boolean, Date, DateTime, BigInteger, ForeignKey")
    output.append("from sqlalchemy.orm import declarative_base, relationship")
    output.append("\nBase = declarative_base()\n")

    processed_types = {}

    # Iterate over all global elements (entry points)
    for element_name, element in schema.elements.items():
        output.append(generate_class_from_element(element, processed_types))

    return "\n".join(output)


# ----------------------------------------------
# 3. Generate a class for an element
# ----------------------------------------------
def generate_class_from_element(
    element: XsdElement,
    processed_types: Dict[str, str]
) -> str:
    """Generate a SQLAlchemy class for the top-level element."""
    cls_name = to_class_name(element.name)

    # If it is a complexType element →
    if isinstance(element.type, XsdComplexType):
        return generate_class_from_complex_type(cls_name, element.type, processed_types)

    return f"# Skipped simple element {element.name}\n"


# ----------------------------------------------
# 4. Generate class for a complex type
# ----------------------------------------------
def generate_class_from_complex_type(
    cls_name: str,
    complex_type: XsdComplexType,
    processed_types: Dict[str, str]
) -> str:

    if cls_name in processed_types:
        return ""  # already generated

    processed_types[cls_name] = cls_name

    lines = []
    lines.append(f"class {cls_name}(Base):")
    lines.append(f"    __tablename__ = '{camel_to_snake(cls_name)}'")
    lines.append("    id = Column(Integer, primary_key=True)\n")

    # Iterate through child elements (sequence, choice, etc.)
    for child in complex_type.content.iter_elements():

        child_name = child.name
        col_name = camel_to_snake(child_name)

        if isinstance(child.type, XsdComplexType):
            # Nested complex type → child table with relationship
            child_class_name = to_class_name(child_name)
            lines.append(
                f"    {col_name}_id = Column(Integer, ForeignKey('{camel_to_snake(child_class_name)}.id'))"
            )
            lines.append(
                f"    {col_name} = relationship('{child_class_name}')"
            )
            # Generate nested class too
            nested = generate_class_from_complex_type(child_class_name, child.type, processed_types)
            lines.append("\n" + nested)

        else:
            # Simple child element
            sqlalchemy_type = map_xsd_type(child.type)

            nullable = "True" if child.min_occurs == 0 else "False"
            lines.append(
                f"    {col_name} = Column({sqlalchemy_type}, nullable={nullable})"
            )

    lines.append("")
    return "\n".join(lines)


# ----------------------------------------------
# 5. Helpers
# ----------------------------------------------
def to_class_name(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))


def camel_to_snake(name: str) -> str:
    out = ""
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            out += "_"
        out += ch.lower()
    return out


# ----------------------------------------------
# 6. Run example
# ----------------------------------------------
if __name__ == "__main__":
    import sys
    xsd_path = sys.argv[1]
    models = generate_sqlalchemy_models(xsd_path)
    print(models)

