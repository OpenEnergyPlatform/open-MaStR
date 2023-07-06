## Map Checker App
A TkInter GUI with a Postgres backend with Mastr. 
### Prerequisites
* PostgreSQL Database with a downloaded Mastr from "date" with the module open-mastr version " "
* POSTGIS Extension 
* Python modules listed in requirements.txt
### How To Use
* Open `config.py` and type edit database connection parameters
* `pyhton MapCheckerApp.py` command should start the application. First time may take a while due to preprocessing. 
### Advanced filters
If you want the behaviour of unit selection, you can edit the function `MapCheckerEngine.fetch_random_unit`
### Validation workflow
Documented in overleaf project
## (Upcoming) 
### CSV Export/Import of validated unit
