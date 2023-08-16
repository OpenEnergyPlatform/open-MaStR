## Get data via the bulk download

On the homepage [MaStR/Datendownload](https://www.marktstammdatenregister.de/MaStR/Datendownload) a zipped folder containing the whole
MaStR is offered. The data is delivered as xml-files. The official documentation can be found 
on the same page (in german). This data is updated on a daily base. 

![Overview of the open_mastr bulk download functionality.](../images/MaStR_bulk_download.svg){ width: 50% align: center }

In the following, the process is described that is started when calling the [`Mastr.download`][open_mastr.Mastr.download] function with the parameter `method`="bulk". 
First, the zipped files are downloaded and saved in `$HOME/.open-MaStR/data/xml_download`. The zipped folder contains many xml files,
which represent the different tables from the MaStR. Those tables are then parsed to a sqlite database. If only some specific
tables are of interest, they can be specified with the parameter `data`. Every table that is selected in `data` will be deleted from the local database, if existent, and then filled with data from the xml files.

In the last step, a basic data cleansing is performed. Many entries in the MaStR from the bulk download are replaced by numbers.
As an example, instead of writing the german states where the unit is registered (Saxony, Brandenburg, Bavaria, ...) the MaStR states 
corresponding digits (7, 2, 9, ...). One major step of cleansing is therefore to replace those digits with their original meaning. 
Moreover, the datatypes of different entries are set in the data cleansing process and corrupted files are repaired.

???+ info "Bulk download trade-offs"
     
    | **Advantages**                            | **Disadvantages**                                |
    |:-------------------------------------------:|:--------------------------------------------------:|
    | No registration for an API key is needed  | No single tables or entries can be downloaded    |
    | Download of the whole dataset is possible | Download takes long time                         |
