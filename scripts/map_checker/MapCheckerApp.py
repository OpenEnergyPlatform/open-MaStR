from tkinter import IntVar, DISABLED, NORMAL
import customtkinter
import numpy as np
from tkintermapview import TkinterMapView
import pandas as pd
from pyproj import Geod

from MapCheckerEngine import MapCheckerEngine
from config import *

customtkinter.set_default_color_theme("blue")


class App(customtkinter.CTk):
    APP_NAME = "Map Checker"
    WIDTH = 800
    HEIGHT = 600

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.title(App.APP_NAME)
        self.geometry(f"{str(App.WIDTH)}x{str(App.HEIGHT)}")
        self.minsize(App.WIDTH, App.HEIGHT)

        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.bind("<Command-q>", self.on_closing)
        self.bind("<Command-w>", self.on_closing)
        self.createcommand("tk::mac::Quit", self.on_closing)

        # Variables
        self.engine = MapCheckerEngine()
        self.int2tech = {1: "solar", 2: "wind", 3: "biomass"}
        self.last_unit = None
        self.last_id = None
        self.last_technology = None

        # Frames

        self.grid_columnconfigure(0, weight=0)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.frame_left = customtkinter.CTkFrame(
            master=self, width=150, corner_radius=0, fg_color=None
        )
        self.frame_left.grid(row=0, column=0, padx=0, pady=0, sticky="nsew")

        self.frame_right = customtkinter.CTkFrame(master=self, corner_radius=0)
        self.frame_right.grid(row=0, column=1, rowspan=1, pady=0, padx=0, sticky="nsew")

        # Empty black space
        self.frame_left.grid_rowconfigure(11, weight=1)

        # Widgets
        # Frame left

        self.label_technology = customtkinter.CTkLabel(
            master=self.frame_left, text="Technology", anchor="w"
        )
        self.label_technology.grid(pady=(10, 0), padx=(20, 20), row=0, column=0)

        # Radio button tech
        self.tech_radio_var = IntVar()
        self.tech_radio_var.set(1)  # default solar

        self.radiobutton_solar = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="Solar",
            variable=self.tech_radio_var,
            value=1,
            command=self.radiobutton_event,
        )
        self.radiobutton_solar.grid(pady=(10, 0), padx=(5, 5), row=1, column=0)

        self.radiobutton_wind = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="Wind",
            variable=self.tech_radio_var,
            value=2,
            command=self.radiobutton_event,
        )
        self.radiobutton_wind.grid(pady=(10, 0), padx=(5, 5), row=1, column=1)

        self.radiobutton_biomass = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="Biomass",
            variable=self.tech_radio_var,
            value=3,
            command=self.radiobutton_event,
        )
        self.radiobutton_biomass.grid(pady=(10, 0), padx=(5, 5), row=1, column=2)

        self.button_fetch_unit = customtkinter.CTkButton(
            master=self.frame_left,
            text="Fetch Random Unit",
            command=self.fetch_unit_event,
        )
        self.button_fetch_unit.grid(pady=(20, 0), padx=(20, 20), row=2, column=0)

        self.label_location = customtkinter.CTkLabel(
            master=self.frame_left, text="Location", anchor="w"
        )
        self.label_location.grid(pady=(5, 0), padx=(20, 20), row=3, column=1)

        self.location_radio_var = IntVar()
        self.location_radio_var.set(1)  # default true

        self.radiobutton_location_true = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="True",
            variable=self.location_radio_var,
            value=1,
            command=self.radiobutton_location_event,
        )
        self.radiobutton_location_true.grid(pady=(0, 0), padx=(5, 5), row=4, column=0)

        self.radiobutton_location_partly = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="Partly True",
            variable=self.location_radio_var,
            value=2,
            command=self.radiobutton_location_event,
        )
        self.radiobutton_location_partly.grid(pady=(0, 0), padx=(5, 5), row=4, column=1)

        self.radiobutton_location_false = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="False",
            variable=self.location_radio_var,
            value=3,
            command=self.radiobutton_location_event,
        )
        self.radiobutton_location_false.grid(pady=(0, 0), padx=(5, 5), row=4, column=2)

        self.label_size = customtkinter.CTkLabel(
            master=self.frame_left, text="Size", anchor="w"
        )
        self.label_size.grid(pady=(20, 0), padx=(20, 20), row=5, column=1)

        self.size_radio_var = IntVar()
        self.size_radio_var.set(1)  # default true

        self.radiobutton_size_true = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="True",
            variable=self.size_radio_var,
            value=1,
            command=self.radiobutton_event,
        )
        self.radiobutton_size_true.grid(pady=(0, 0), padx=(5, 5), row=6, column=0)

        self.radiobutton_size_false = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="False",
            variable=self.size_radio_var,
            value=2,
            command=self.radiobutton_event,
        )
        self.radiobutton_size_false.grid(pady=(0, 0), padx=(5, 5), row=6, column=1)

        self.button_OK = customtkinter.CTkButton(
            master=self.frame_left,
            text="Yes (Y)",
            command=self.ok_event,
            fg_color="green",
        )

        self.label_orientation = customtkinter.CTkLabel(
            master=self.frame_left, text="Orientation", anchor="w"
        )
        self.label_orientation.grid(pady=(20, 0), padx=(20, 20), row=7, column=1)

        self.orientation_radio_var = IntVar()
        self.orientation_radio_var.set(1)  # default true

        self.radiobutton_orientation_true = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="True",
            variable=self.orientation_radio_var,
            value=1,
            command=self.radiobutton_event,
        )
        self.radiobutton_orientation_true.grid(
            pady=(0, 0), padx=(5, 5), row=8, column=0
        )

        self.radiobutton_orientation_false = customtkinter.CTkRadioButton(
            master=self.frame_left,
            text="False",
            variable=self.orientation_radio_var,
            value=2,
            command=self.radiobutton_event,
        )
        self.radiobutton_orientation_false.grid(
            pady=(0, 0), padx=(5, 5), row=8, column=1
        )

        self.button_OK = customtkinter.CTkButton(
            master=self.frame_left,
            text="OK",
            command=self.ok_event,
        )

        self.button_OK.grid(pady=(5, 0), padx=(20, 20), row=9, column=0)
        self.bind("o", lambda e: self.ok_event())

        self.unit_info_label = customtkinter.CTkLabel(
            self.frame_left, text="Unit Info", anchor="w", bg_color="#1f6aa5"
        )
        self.unit_info_label.grid(
            pady=(10, 10), padx=(20, 20), row=10, column=0, columnspan=3
        )

        self.map_label = customtkinter.CTkLabel(
            self.frame_left, text="Tile Server:", anchor="w"
        )
        self.map_label.grid(row=12, column=0, padx=(20, 20), pady=(20, 0))
        self.map_option_menu = customtkinter.CTkOptionMenu(
            self.frame_left,
            values=[
                "OpenStreetMap",
                "Google normal",
                "Google satellite",
                "Google hybrid",
            ],
            command=self.change_map,
        )
        self.map_option_menu.grid(row=13, column=0, padx=(20, 20), pady=(10, 20))

        # Frame right

        self.frame_right.grid_rowconfigure(1, weight=1)
        self.frame_right.grid_rowconfigure(0, weight=0)
        self.frame_right.grid_columnconfigure(0, weight=1)
        self.frame_right.grid_columnconfigure(1, weight=0)
        self.frame_right.grid_columnconfigure(2, weight=1)

        self.map_widget = TkinterMapView(self.frame_right, corner_radius=0)
        self.map_widget.grid(
            row=1,
            rowspan=1,
            column=0,
            columnspan=3,
            sticky="nswe",
            padx=(0, 0),
            pady=(0, 0),
        )

        self.entry = customtkinter.CTkEntry(
            master=self.frame_right, placeholder_text="Type address"
        )
        self.entry.grid(row=0, column=0, sticky="we", padx=(12, 0), pady=12)
        self.entry.entry.bind("<Return>", self.search_event)

        self.button_search = customtkinter.CTkButton(
            master=self.frame_right, text="Search", width=90, command=self.search_event
        )
        self.button_search.grid(row=0, column=1, sticky="w", padx=(12, 0), pady=12)

        # Set default values
        self.map_widget.set_address("Berlin")
        self.map_option_menu.set("Google satellite")
        customtkinter.set_appearance_mode("dark")

    # Events

    def radiobutton_event(self):
        pass

    def radiobutton_location_event(self):
        if self.location_radio_var.get() == 3:
            self.orientation_radio_var.set(2)
            self.size_radio_var.set(2)

    def fetch_unit_event(self):

        # Reset
        self.remove_all_canvas_objects()
        self.location_radio_var.set(1)

        technology = self.int2tech[self.tech_radio_var.get()]
        if technology == "solar":
            self.size_radio_var.set(1)
            self.orientation_radio_var.set(1)
            if self.last_technology == "wind":
                # switch on
                self.radiobutton_orientation_false.configure(state=NORMAL)
                self.radiobutton_orientation_true.configure(state=NORMAL)
                self.radiobutton_size_false.configure(state=NORMAL)
                self.radiobutton_size_true.configure(state=NORMAL)
        elif technology in ["wind", "biomass"]:
            self.radiobutton_orientation_false.configure(state=DISABLED)
            self.radiobutton_orientation_true.configure(state=DISABLED)
            self.radiobutton_size_false.configure(state=DISABLED)
            self.radiobutton_size_true.configure(state=DISABLED)

        # Get unit

        unit_df = self.engine.fetch_random_unit(technology=technology)
        self.last_unit = unit_df
        self.last_id = unit_df["EinheitMastrNummer"][0]
        self.last_technology = technology

        self.map_widget.set_zoom(19)

        # Show on map
        marker = self.map_widget.set_position(
            deg_x=unit_df["Breitengrad"][0],
            deg_y=unit_df["Laengengrad"][0],
            marker=True,
            text=f"ID: {self.last_id} \n{unit_df['Bruttoleistung'][0]} kW \n{unit_df['Hauptausrichtung'][0]}"
            if technology == "solar"
            else f"ID: {self.last_id} \n {unit_df['Bruttoleistung'][0]} kW",
            text_color="white",
        )
        # Get neighbor units
        neighbors_df = self.engine.fetch_neighbors(
            unit_id=self.last_id, technology=technology
        )
        # Show neighbor units on map
        for idx, unit in neighbors_df.iterrows():
            marker = self.map_widget.set_marker(
                deg_x=unit["Breitengrad"] - 0.00003 * (idx + 1),
                deg_y=unit["Laengengrad"],
                text=f"{unit['EinheitMastrNummer']} \n {unit['Bruttoleistung']} kW",
                text_color="white",
                marker_color_outside="green",
                marker_color_circle="white",
            )
        if technology == "solar":
            box_polygon = self.unit_to_estimated_box(unit_df)
            self.map_widget.set_polygon(
                box_polygon, fill_color=None, outline_color="red", border_width=6
            )
        elif technology == "wind":
            octagon_polygon = self.unit_to_octagon(unit_df, TOLERANCE_RADUIS_TRUE)
            self.map_widget.set_polygon(
                octagon_polygon, fill_color=None, outline_color="green", border_width=6
            )
            octagon_polygon = self.unit_to_octagon(
                unit_df, TOLERANCE_RADUIS_PARTLY_TRUE
            )
            self.map_widget.set_polygon(
                octagon_polygon, fill_color=None, outline_color="orange", border_width=3
            )

        # Display info of unit
        unit_info = unit_df.T.to_string(header=False)
        self.unit_info_label.configure(text=unit_info, justify="left")

    def ok_event(self):
        self.engine.set_validity(
            self.last_unit["EinheitMastrNummer"][0],
            technology=self.last_technology,
            manval_location=self.location_radio_var.get(),
            manval_size=self.size_radio_var.get(),
            manval_orientation=self.orientation_radio_var.get(),
        )

        self.fetch_unit_event()

    def marker_click_event(self):
        pass

    def get_center_position(self):
        t = self.map_widget.get_position()
        print(t)

    def search_event(self, event=None):
        self.map_widget.set_address(self.entry.get())

    # utils

    def change_map(self, new_map: str):
        if new_map == "OpenStreetMap":
            self.map_widget.set_tile_server(
                "https://a.tile.openstreetmap.org/{z}/{x}/{y}.png"
            )
        elif new_map == "Google normal":
            self.map_widget.set_tile_server(
                "https://mt0.google.com/vt/lyrs=m&hl=en&x={x}&y={y}&z={z}&s=Ga",
                max_zoom=22,
            )
        elif new_map == "Google satellite":
            self.map_widget.set_tile_server(
                "https://mt0.google.com/vt/lyrs=s&hl=en&x={x}&y={y}&z={z}&s=Ga",
                max_zoom=25,
            )
        elif new_map == "Google hybrid":
            self.map_widget.set_tile_server(
                "https://mt0.google.com/vt/lyrs=y&hl=en&x={x}&y={y}&z={z}&s=Ga",
                max_zoom=25,
            )

    def unit_to_estimated_box(self, unit_df):
        lat = unit_df["Breitengrad"]
        lon = unit_df["Laengengrad"]
        power = unit_df["Bruttoleistung"]  # in kW
        # Faustregel 1kWp ~ 5.5 m2
        unit_area = (
            power * 5.5
        )  # https://www.dachvermieten.net/wieviel-qm-dachflaeche-fuer-1-kwp-kilowattpeak/
        half_diag_length = (
            np.sqrt(unit_area) * np.sqrt(2) / 2
        )  # from center to corner length in meter

        g = Geod(ellps="WGS84")  # Create a geodesic calculation object

        polygon_list = [
            g.fwd(lon, lat, angle, half_diag_length) for angle in [45, 135, 225, 315]
        ]
        polygon_list = [
            (coord[1][0], coord[0][0]) for coord in polygon_list
        ]  # x,y swap

        return polygon_list

    def unit_to_octagon(self, unit_df, radius):
        lat = unit_df["Breitengrad"]
        lon = unit_df["Laengengrad"]

        g = Geod(ellps="WGS84")  # Create a geodesic calculation object

        polygon_list = [g.fwd(lon, lat, angle, radius) for angle in range(0, 360, 45)]

        polygon_list = [
            (coord[1][0], coord[0][0]) for coord in polygon_list
        ]  # x,y swap

        return polygon_list

    def remove_all_canvas_objects(self):

        # markers
        for _ in range(len(self.map_widget.canvas_marker_list)):
            marker = self.map_widget.canvas_marker_list.pop()
            self.map_widget.delete(marker)

        # Polygons
        for _ in range(len(self.map_widget.canvas_polygon_list)):
            polygon = self.map_widget.canvas_polygon_list.pop()
            self.map_widget.delete(polygon)

    def on_closing(self, event=0):
        self.destroy()

    def start(self):
        self.mainloop()


if __name__ == "__main__":
    app = App()
    app.start()
