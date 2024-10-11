#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Matches wind turbines to the wind_turbine_library
https://openenergy-platform.org/dataedit/view/supply/wind_turbine_library
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "solar-c"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.10.0"


import pandas as pd
import os


def read_csv_turbine(csv_name):
    turbines = pd.read_csv(
        csv_name,
        header=0,
        encoding="utf-8",
        sep=",",
        error_bad_lines=True,
        index_col=False,
        dtype={
            "index": int,
            "id": int,
            "turbine_id": int,
            "manufacturer": str,
            "name": str,
            "turbine_type": str,
            "nominal_power": str,
            "rotor_diamter": str,
            "rotor_area": str,
            "hub_height": str,
            "max_speed_drive": str,
            "wind_class_iec": str,
            "wind_zone_dibt": str,
            "power_density": str,
            "power_density_2": str,
            "calculated": str,
            "has_power_curve": str,
            "power_curve_wind_speeds": str,
            "power_curve_values": str,
            "has_cp_curve": str,
            "power_coefficient_curve_wind_speeds": str,
            "power_coefficient_curve_values": str,
            "has_ct_curve": str,
            "thrust_coefficient_curve_wind_speeds": str,
            "thrust_coefficient_curve_values": str,
            "source": str,
        },
    )
    return turbines


def create_dataset(df):
    types = []
    for i, r in df.iterrows():
        types.append(prepare_turbine_type(r))
    df.insert(6, "turbine_type_v2", types)
    write_to_csv(df, "turbine_library_t.csv")


def write_to_csv(df, path):
    with open(path, mode="a", encoding="utf-8") as file:
        df.to_csv(
            file,
            sep=",",
            mode="a",
            header=file.tell() == 0,
            line_terminator="\n",
            encoding="utf-8",
        )


def prepare_turbine_type(turbine):
    nom_pow = turbine.nominal_power
    diam = turbine.rotor_diameter
    man = get_manufacturer_short(turbine.manufacturer, nom_pow, diam)
    type_name = man + "-" + str(diam) + "_" + str(int(nom_pow))
    return type_name


def get_manufacturer_short(manufacturer, nom_pow, diam):
    man = ""
    if manufacturer == "Nordex":
        man = "N"
        if int(nom_pow) == 3000 or int(nom_pow) == 1500:
            if (
                int(diam) == 140
                or int(diam) == 132
                or int(diam) == 125
                or int(diam) == 116
                or int(diam) == 100
                or int(diam) == 82
                or int(diam) == 77
                or int(diam) == 70
            ):
                man = "AW"
    elif manufacturer == "Adwen/Areva":
        man = "AD"
    elif manufacturer == "Senvion/REpower":
        man = "S"
        if int(nom_pow) == 2050 or int(nom_pow) == 2000:
            man = "MM"
    elif manufacturer == "Enercon":
        man = "E"
    elif manufacturer == "Siemens":
        man = "SWT"
    elif manufacturer == "Vestas":
        man = "V"
    elif manufacturer == "Vensys":
        man = "VS"
    elif manufacturer == "GE Wind":
        man = "GE"
    elif manufacturer == "Eno":
        man = "ENO"
    elif manufacturer == "aerodyn":
        man = "SCD"
    return man
