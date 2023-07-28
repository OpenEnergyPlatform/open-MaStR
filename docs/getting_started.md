# Getting Started


The intention of open-MaStR is to provide tools for receiving a complete as possible and accurate as possible list of
power plant units based on the public registry Marktstammdatenregister (short: [MaStR](https://www.marktstammdatenregister.de)).

## Downloading the MaStR data


For downloading the MaStR and saving it in a sqlite database, you will use the `Mastr` class and its `download` method (For documentation of those methods see [mastr module](#mastr-module))

The `download` function offers two different ways to get the data by changing the `method` parameter (if not specified, `method` defaults to "bulk"):

1. `method` = "bulk": Get data via the bulk download from [MaStR/Datendownload](https://www.marktstammdatenregister.de/MaStR/Datendownload)
2. `method` = "API": Get data via the MaStR-API

Keep in mind: While the data from both methods is quiet similar, it is not exactly the same!

### Bulk download

The following code block shows the basic download commands:

```python
from open_mastr import Mastr

db = Mastr()
db.download()
```

When a `Mastr` object is initialized, a sqlite database is created in `$HOME/.open-MaStR/data/sqlite`. With the function `Mastr.download()`, the **whole MaStR is downloaded** in the zipped xml file format. It is then read into the sqlite database and simple data cleansing functions are started.

More detailed information can be found in [Get data via the bulk download](#get-data-via-the-bulk-download).

API download
-----------------------------------
When using `download(method="API")`, the data is retrieved from the MaStR API. For using the MaStR API, credentials are needed (see [Get data via the MaStR-API](#get-data-via-the-mastr-api)).

```python
from open_mastr import Mastr

db = Mastr()
db.download(method='API')
```

The default settings will save retrieved data into the sqlite database. The function can be used to mirror the open-MaStR database regularly without needing to download the provided dumps daily.

## Accessing the database



For accessing and working with the MaStR database after you have downloaded it, you can use sqlite browsers 
such as [DB Browser for SQLite](https://sqlitebrowser.org/) or any python module
which can process sqlite data. Pandas, for example, comes with the function
[read_sql](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html).

```python
import pandas as pd

table="wind_extended"
df = pd.read_sql(sql=table, con=db.engine)
```


## Exporting data

The tables in the database can be exported as csv files. While technology-related data is joined for each unit,
additional tables are mirrored from database to csv as they are. To export the data you can use to method `to_csv`

```python

    tables=["wind", "grid"]
    db.to_csv(tables)
```