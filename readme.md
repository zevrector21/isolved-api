# Isolve API
Integrating Isolved API to fetch data and store into the SQL server.
https://myisolved.com

## Features
- Connect to the SQL database server
- Create the tables for the employees and checks data.
- Get the Isolve API token via Oauth2.0
- Fetch the Employees and Checks data from the APIs
- Save the employees data every day
- Save the checks data if it's new
- Cron the script to run everyday

## Tech
- Python - python requests to call the apis
- ODBC driver 18 - sql server connector

## Installation
- Setup the ODBC driver on the machine
    - Windows <br>
        https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
    - Linux <br>
        https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15&tabs=ubuntu18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline

- Setup python and pip
    - Windows <br>
        https://www.python.org/downloads/
    - Linux  <br>
        https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-programming-environment-on-ubuntu-20-04-quickstart

- Install python libraries
```
git clone https://github.com/zevrector21/isolved-api.git
cd isolved-api
pip install -r requirements.txt
```

- Config the environment variables
Rename the .env.example to .env and provide the credentials for sql server and isolved API.

## Development

- For the employees data 
```
python run_me details
```
- For the Checks data
```
python run_me.py checks
```

## Production

- For the employees data
```
nohup python run_me.py details &
```
- For the Checks data
```
nohup python run_me.py checks &
```

## License
MIT
