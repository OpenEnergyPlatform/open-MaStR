from bokeh.palettes import Category10_10 as palette

# import geoviews as gv
import bokeh

# gv.extension('bokeh')


def plotPowerPlants(df):
    # size marker according to gross power output
    iMaxSize = 30
    iMinSize = 10
    df["size"] = (df["Bruttoleistung"] - df["Bruttoleistung"].min()) / (
        df["Bruttoleistung"].max() - df["Bruttoleistung"].min()
    ) * (iMaxSize - iMinSize) + iMinSize

    # convert datetime to string
    df["date"] = df["Inbetriebnahmedatum"].dt.strftime("%Y-%m-%d")

    # define tooltips
    tt = [
        ("Name", "@Name"),
        ("Address", "@Standort, @Bundesland, @Land"),
        ("Commissioning date", "@date"),
        ("Type", "@Einheittyp"),
        ("Gross generation", "@Bruttoleistung MW"),
    ]
    hover_tool = bokeh.models.HoverTool(tooltips=tt)

    # choose background tiles
    overlay = gv.tile_sources.Wikipedia

    # markers will be colored according to Einheittyp
    groups = df["Einheittyp"].unique()
    colors = {
        "Wasser": palette[0],
        "Biomasse": palette[2],
        "Windeinheit": palette[4],
    }

    # create geoviews point objects
    for group in groups:
        df_group = df.loc[
            df["Einheittyp"] == group,
            [
                "Name",
                "Standort",
                "Bundesland",
                "Land",
                "date",
                "Einheittyp",
                "Bruttoleistung",
                "Laengengrad",
                "Breitengrad",
                "size",
            ],
        ]
        points = gv.Points(
            df_group, ["Laengengrad", "Breitengrad"], label=group
        ).options(
            aspect=2,
            responsive=True,
            tools=[hover_tool],
            size="size",
            active_tools=["wheel_zoom"],
            fill_alpha=0.6,
            fill_color=colors[group],
            line_color="white",
        )
        overlay = overlay * points

    # hide group when clicking on legend
    overlay.options(click_policy="hide", clone=False)

    # return figure
    return overlay
