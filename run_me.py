import requests
import csv
import pdb
import json
import pyodbc
import time
from datetime import date, datetime
import logging
import sys
import os
from dotenv import dotenv_values
import schedule


class Main:
    names = ["details", "checks"]
    api_endpoint = "https://snfpayroll.myisolved.com/rest/api"
    debug = False
    exception_list = ["beecan health llc", "beecan health co llc"]
    exception_code_list = ["BHC", "BHCO"]
    thread_count = 10

    def __init__(self):
        self.config = dotenv_values(".env")
        if sys.argv[1] in self.names:
            self.name = sys.argv[1]
            if len(sys.argv) > 2:
                self.begin_at = int(sys.argv[2])
                self.page_num = int(sys.argv[3])
            else:
                self.begin_at = 0
                self.page_num = 0
            print(f"It's running for {self.name} from {self.begin_at} - {self.page_num}...")
        else:
            print("The command is out of control. Try with checks or details, please.")
            exit(0)
        self.session = requests.Session()
        self.setup_log()
        self.count = 0
        self.token = self.get_token()
        self.connect_database()
        if self.debug:
            self.csv_writer = self.get_writer()
        self.prev_time = time.time()
        if self.name == "checks":
            while True:
                self.start_requests()
        else:
            self.start_requests()
            schedule.every().day.at("06:00").do(self.start_requests)
            while True:
                schedule.run_pending()
        self.disconnect_database()

    def start_requests(self):
        client_list = self.get_client_list() # [83, 96]
        for client in client_list[self.begin_at:]:
            client_details = self.get_client_details(client)
            self.client_organizations = {}
            for organization in client_details.get("organizations", []):
                o_key = self.validate(organization.get("title"))
                o_value = {}
                for lookup in organization.get("lookups", []):
                    o_value[lookup["code"]] = {
                        "code": lookup["code"],
                        "description": lookup["description"]
                    }
                self.client_organizations[o_key] = o_value

            self.client_legals = {}
            for legal in client_details.get("legalCompanies", []):
                l_key = self.validate(legal.get("legalCode"))
                l_value = self.validate(legal.get("legalName"))
                self.client_legals[l_key] = l_value

            try:
                page_url = None
                for link in client.get("links", []):
                    if link["rel"] == "self":
                        page_url = f"{link['href']}/employees"
                        break

                if self.page_num != 0:
                    page_url = f"https://snfpayroll.myisolved.com/rest/api/clients/{client.get('id')}/employees?page={self.page_num}"

                while page_url:
                    logging.info(f"client_id: {self.validate(client.get('id'))} | page_url: {page_url}")

                    response = self.session.get(
                        url = page_url, 
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {self.token['access_token']}",
                        }
                    )
                    if response.status_code == 200:
                        data = response.json()
                        employee_list = data.get("results", [])

                        for employee in employee_list:
                            if self.name == "checks":
                                facility_code = employee.get("legalCode")
                                if "BHC" in facility_code or "BHCO" in facility_code:
                                    logging.warning(f"exception_facility: employee_id: {employee.get('id')} | legal_code: {facility_code}")
                                    continue

                            cur_time = time.time()
                            if cur_time - self.prev_time > 240:
                                self.token = self.get_token()
                                self.prev_time = cur_time

                            self.parse_employee(employee)

                        page_url = data["nextPageUrl"]
                    else:
                        logging.error(f"get_employee_list: {response.status_code}: {response.content}")
                        break

            except Exception as e:
                logging.exception(f"get_employee_list: {e}")

    def parse_employee(self, employee):
        jobs = self.get_employee_jobs(employee)
        if self.name == "details":
            employee_details = self.get_employee_details(employee, jobs)
            self.insert_employee_details(employee_details)
        else:
            employee_check_list = self.get_employee_check_list(employee)
            for employee_check in employee_check_list:
                cur_time = time.time()
                if cur_time - self.prev_time > 260:
                    self.token = self.get_token()
                    self.prev_time = cur_time

                employee_check_details = self.get_employee_check_details(employee_check, jobs)
                self.insert_employee_checks(employee, employee_check_details)

    # Get all the clients
    def get_client_list(self):
        client_list = []
        try:
            response = self.session.get(
                url = f"{self.api_endpoint}/clients", 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                client_list = response.json()["results"]
            else:
                logging.error(f"get_client_list: {response.status_code}: {response.content}")

        except Exception as e:
            logging.exception(f"get_client_list: {e}")

        # logging.info(f"get_client_list: {len(client_list)}")
        return client_list

    # Get the client details
    def get_client_details(self, client):
        client_details = {}
        try:
            response = self.session.get(
                url = f"{self.api_endpoint}/clients/{client['id']}?includeDetails=True", 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                client_details = response.json()
            else:
                logging.error(f"get_client_details: {response.status_code}: {response.content}")

        except Exception as e:
            logging.exception(f"get_client_details: {e}")

        # logging.info(f"get_client_details")
        return client_details

    # Get the legal list by client
    def get_legal_list(self, client):
        legal_list = []
        try:
            response = self.session.get(
                url = f"{self.api_endpoint}/clients/{client['id']}/legals", 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                legal_list = response.json()
            else:
                logging.error(f"get_legal_list: {response.status_code}: {response.content}")

        except Exception as e:
            logging.exception(f"get_legal_list: {e}")

        # logging.info(f"get_legal_list: {len(legal_list)}")
        return legal_list

    # Get the legal details
    def get_legal_details(self, client):
        legal_details = {}
        try:
            response = self.session.get(
                url = f"{self.api_endpoint}/clients/{client['id']}?includeDetails=True", 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                legal_details = response.json()
            else:
                logging.error(f"get_legal_details: {response.status_code}: {response.content}")

        except Exception as e:
            logging.exception(f"get_legal_details: {e}")

        # logging.info(f"get_legal_details")
        return legal_details

    # Get the employees list by legal
    def get_legal_employee_list(self, legal):
        employee_list = []
        try:
            page_url = None
            for link in legal.get("links", []):
                if link["rel"] == "Employees":
                    page_url = link["href"]
                    break

            while page_url:
                response = self.session.get(
                    url = page_url, 
                    headers = {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.token['access_token']}",
                    }
                )
                if response.status_code == 200:
                    data = response.json()
                    employee_list += data["results"]
                    page_url = data["nextPageUrl"]
                else:
                    logging.error(f"get_legal_employee_list: {response.status_code}: {response.content}")
                    break

        except Exception as e:
            logging.exception(f"get_legal_employee_list: {e}")

        # logging.info(f"get_legal_employee_list: {len(employee_list)}")
        return employee_list

    # Get the employee details
    def get_employee_details(self, employee, jobs):
        employee_details = {}
        try:
            employee_link = None
            for link in employee.get("links", []):
                if link["rel"] == "self":
                    employee_link = link["href"]
                    break

            response = self.session.get(
                url = employee_link, 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                employee_details = response.json()
            else:
                logging.error(f"get_employee_details: {response.status_code}: {response.content}")

            organizations = []
            if len(jobs) > 0 and jobs[0].get("organizations"):
                organizations = jobs[0].get("organizations") or []
                for organization in organizations:
                    key = self.validate(organization.get("clientOrganizationField", {}).get("title"))
                    value = self.validate(organization.get("organizationValue"))
                    if key == "" or value == "":
                        continue
                    employee_details[key] = self.client_organizations[key][value]
            else:
                employee_check_list = self.get_employee_check_list(employee)
                employee_check_details = {}
                for employee_check in employee_check_list:
                    employee_check_details = self.get_employee_check_details(employee_check, [])
                    if employee_check_details:
                        break
                organizations = employee_check_details.get("employeeOrganizations") or []
                for organization in organizations:
                    key = self.validate(organization.get("title"))
                    value = self.validate(organization.get("value"))
                    if key == "" or value == "":
                        continue
                    employee_details[key] = self.client_organizations[key][value]

        except Exception as e:
            logging.exception(f"get_employee_details: {e}")

        # logging.info(f"get_employee_details")
        return employee_details

    # Get the checks by employee
    def get_employee_check_list(self, employee):
        employee_check_list = []
        try:
            page_url = None
            for link in employee.get("links", []):
                if link["rel"] == "Checks":
                    page_url = link["href"]
                    break

            while page_url:
                response = self.session.get(
                    url = page_url, 
                    headers = {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.token['access_token']}",
                    }
                )
                if response.status_code == 200:
                    data = response.json()
                    employee_check_list += data["results"]
                    page_url = data["nextPageUrl"]
                else:
                    logging.error(f"get_employee_check_list: {response.status_code}: {response.content}")
                    break

        except Exception as e:
            logging.exception(f"get_employee_check_list: {e}")

        logging.info(f"get_employee_check_list: {len(employee_check_list)}")
        return employee_check_list

    # Get the employee check details
    def get_employee_check_details(self, employee_check, jobs):
        employee_check_details = {}
        try:
            employee_check_link = None
            for link in employee_check.get("links", []):
                if link["rel"] == "self":
                    employee_check_link = link["href"]
                    break

            response = self.session.get(
                url = employee_check_link, 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                employee_check_details = response.json()
            else:
                logging.error(f"get_employee_check_details: {response.status_code}: {response.content}")

            organizations = []
            if len(jobs) > 0 and jobs[0].get("organizations"):
                organizations = jobs[0].get("organizations") or []
                for organization in organizations:
                    key = self.validate(organization.get("clientOrganizationField", {}).get("title"))
                    value = self.validate(organization.get("organizationValue"))
                    if key == "" or value == "":
                        continue
                    employee_check_details[key] = self.client_organizations[key][value]
            else:
                organizations = employee_check_details.get("employeeOrganizations") or []
                for organization in organizations:
                    key = self.validate(organization.get("title"))
                    value = self.validate(organization.get("value"))
                    if key == "" or value == "":
                        continue
                    employee_check_details[key] = self.client_organizations[key][value]

        except Exception as e:
            logging.exception(f"get_employee_check_details: {e}")

        # logging.info(f"get_employee_check_details")
        return employee_check_details

    # Get the jobs by employee
    def get_employee_jobs(self, employee,):
        jobs = []
        try:
            employee_link = None
            for link in employee.get("links", []):
                if link["rel"] == "self":
                    employee_link = link["href"]
                    break

            response = self.session.get(
                url = f"{employee_link}/jobs", 
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token['access_token']}",
                }
            )
            if response.status_code == 200:
                jobs = response.json()
            else:
                logging.error(f"get_employee_jobs: {response.status_code}: {response.content}")

        except Exception as e:
            logging.exception(f"get_employee_jobs: {e}")

        # logging.info(f"get_employee_jobs")
        return jobs

    # Retrived the token from OAuth2
    def get_token(self):
        token = {}
        try:
            client_auth = requests.auth.HTTPBasicAuth(
                self.config.get("client_id"),
                self.config.get("client_secret")
            )
            response = requests.post(
                url = f"{self.api_endpoint}/token",
                auth = client_auth,
                data = {
                    "grant_type": "client_credentials",
                }
            )
            if response.status_code == 200:
                token = response.json()
                logging.info(f"get_token")
                return token
            else:
                logging.error(f"get_token: {response.status_code}: {response.content}")

        except Exception as e:
            logging.exception(f"get_token: {e}")
        
        exit(0)

    # Retrived the refresh_token from OAuth2
    def get_refresh_token(self):
        refresh_token = {}
        try:
            initial_token = self.get_token()
            response = requests.post(
                url = f"{self.api_endpoint}/token",
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": initial_token['refresh_token'],
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
            )
            if response.status_code == 200:
                refresh_token = response.json()
            else:
                logging.info(f"get_refresh_token: {response.status_code}: {response.content}")

        except Exception as e:
            logging.info(f"get_refresh_token: {e}")

        return refresh_token

    # Create the Azure sql database connection
    def connect_database(self):
        server = self.config.get('server')
        database = self.config.get('database')
        username = self.config.get('username')
        password = self.config.get('password')
        driver= self.config.get('driver')
        self.conn = pyodbc.connect(f"DRIVER={driver};PORT=1433;SERVER={server};PORT=1443;DATABASE={database};UID={username};PWD={password}")
        self.cursor = self.conn.cursor()

        # Create employee_list_type_1 table if not exist
        self.cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='employee_list_type_1' AND xtype='U')
            CREATE TABLE employee_list_type_1 (
                id int Identity primary key NOT NULL,
                facility_name nvarchar(100),
                department nvarchar(100),
                department_code nvarchar(100),
                employee_first_name nvarchar(100),
                employee_middle_name nvarchar(100),
                employee_last_name nvarchar(100),
                hire_date date,
                rehire_date date,
                termination_date date,
                leave_date date,
                seniority_date date,
                position nvarchar(100),
                position_id nvarchar(100),
                system_id nvarchar(100),
                employee_id nvarchar(100),
                status nvarchar(100),
                status_type nvarchar(100),
                email nvarchar(100),
                pay_type nvarchar(100),
                hourly_rate float,
                load_date date
            )
       ''')
        self.conn.commit()

        # Create employee_list_type_2 table if not exist
        self.cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='employee_list_type_2' AND xtype='U')
            CREATE TABLE employee_list_type_2 (
                id int Identity primary key NOT NULL,
                facility_name nvarchar(100),
                department nvarchar(100),
                department_code nvarchar(100),
                employee_first_name nvarchar(100),
                employee_middle_name nvarchar(100),
                employee_last_name nvarchar(100),
                hire_date date,
                rehire_date date,
                termination_date date,
                leave_date date,
                seniority_date date,
                position nvarchar(100),
                position_id nvarchar(100),
                system_id nvarchar(100),
                employee_id nvarchar(100),
                status nvarchar(100),
                status_type nvarchar(100),
                email nvarchar(100),
                pay_type nvarchar(100),
                load_date date
            )
       ''')
        self.conn.commit()

        # Create employee_checks table if not exist
        self.cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='employee_checks' AND xtype='U')
            CREATE TABLE employee_checks (
                id int Identity primary key NOT NULL,
                facility_name nvarchar(100),
                department nvarchar(100),
                department_code nvarchar(100),
                employee_first_name nvarchar(100),
                employee_last_name nvarchar(100),
                position nvarchar(100),
                position_code nvarchar(100),
                system_id nvarchar(100),
                employee_id nvarchar(100),
                hours float,
                dollars float,
                earning_code nvarchar(100),
                earning_group nvarchar(100),
                check_date date,
                period_end_date date,
                check_type nvarchar(100),
                check_number nvarchar(100),
                load_date date
            )
       ''')
        self.conn.commit()

    # Close the Azure sql database connection
    def disconnect_database(self):
        self.cursor.close()
        self.conn.close()

    # Insert employee checks into database
    def insert_employee_details(self, employee_details):
        today = date.today().strftime('%Y-%m-%d')
        try:
            facility_name = self.client_legals.get(self.validate(employee_details.get("legalCode"))) or ""
            system_id = self.validate(employee_details.get("id"))
            employee_id = self.validate(employee_details.get("employeeNumber"))
            if facility_name != "" and facility_name.lower() not in self.exception_list:
                self.cursor.execute(f"""
                    IF NOT EXISTS (SELECT * FROM employee_list_type_1 WHERE system_id='{system_id}' and load_date='{today}')
                    INSERT employee_list_type_1 (
                        facility_name, department, department_code, employee_first_name,
                        employee_middle_name, employee_last_name, hire_date, rehire_date,
                        termination_date, leave_date, seniority_date, position,
                        position_id, system_id, employee_id, status, status_type,
                        email, pay_type, hourly_rate, load_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 

                    self.validate(facility_name),
                    self.validate(employee_details.get("Department", {}).get("description")),
                    self.validate(employee_details.get("Department", {}).get("code")),
                    self.validate(employee_details.get("nameAddress", {}).get("firstName")),
                    self.validate(employee_details.get("nameAddress", {}).get("middleName")),
                    self.validate(employee_details.get("nameAddress", {}).get("lastName")),
                    self.validate(employee_details.get("hireDate"), "datetime"),
                    self.validate(employee_details.get("rehireDate"), "datetime"),
                    self.validate(employee_details.get("terminationDate"), "datetime"),
                    None,
                    None,
                    self.validate(employee_details.get("Position", {}).get("description")),
                    self.validate(employee_details.get("Position", {}).get("code")),
                    system_id,
                    employee_id,
                    self.get_employee_status_code(self.validate(employee_details.get("employmentStatus"))),
                    self.validate(employee_details.get("employmentCategoryCode")),
                    self.validate(employee_details.get("emailAddress")),
                    self.validate(employee_details.get("payType")),
                    self.validate(employee_details.get("hourlyRate"), "number"),
                    today
                )
            else:
                self.cursor.execute(f"""
                    IF NOT EXISTS (SELECT * FROM employee_list_type_2 WHERE system_id='{system_id}' and load_date='{today}')
                    INSERT employee_list_type_2 (
                        facility_name, department, department_code, employee_first_name,
                        employee_middle_name, employee_last_name, hire_date, rehire_date,
                        termination_date, leave_date, seniority_date, position,
                        position_id, system_id, employee_id, status, status_type,
                        email, pay_type, load_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 

                    self.validate(facility_name),
                    self.validate(employee_details.get("Department", {}).get("description")),
                    self.validate(employee_details.get("Department", {}).get("code")),
                    self.validate(employee_details.get("nameAddress", {}).get("firstName")),
                    self.validate(employee_details.get("nameAddress", {}).get("middleName")),
                    self.validate(employee_details.get("nameAddress", {}).get("lastName")),
                    self.validate(employee_details.get("hireDate"), "datetime"),
                    self.validate(employee_details.get("rehireDate"), "datetime"),
                    self.validate(employee_details.get("terminationDate"), "datetime"),
                    None,
                    None,
                    self.validate(employee_details.get("Position", {}).get("description")),
                    self.validate(employee_details.get("Position", {}).get("code")),
                    system_id,
                    employee_id,
                    self.get_employee_status_code(self.validate(employee_details.get("employmentStatus"))),
                    self.validate(employee_details.get("employmentCategoryCode")),
                    self.validate(employee_details.get("emailAddress")),
                    self.validate(employee_details.get("payType")),
                    today
                )
            self.conn.commit()
            if self.count / 100 == 0:
                logging.info(f"counter: {self.count} | legal_code: {employee_details.get('legalCode')} | facility_name: {facility_name}")
            self.count += 1
        except Exception as e:
            logging.exception(f"insert_employee_details: {e}")
            time.sleep(60)
            logging.exception(f"insert_employee_details: connecting database again")
            self.connect_database()
            self.insert_employee_details(employee_details)

    # Insert employee checks into database
    def insert_employee_checks(self, employee, employee_check_details):
        system_id = self.validate(employee_check_details.get("id"))
        employee_id = self.validate(employee_check_details.get("employeeNumber"))
        for earning in employee_check_details.get("garnishments") or []:
            self.add_query(employee, employee_check_details, system_id, employee_id, "Garnishments", 
                self.validate(earning.get("itemCode")), 
                self.validate(earning.get("checkHours"), "number"),
                self.validate(earning.get("checkDollars"), "number")
            )

        for earning in employee_check_details.get("deductions") or []:
            self.add_query(employee, employee_check_details, system_id, employee_id, "Deductions", 
                self.validate(earning.get("itemCode")), 
                self.validate(earning.get("checkHours"), "number"),
                self.validate(earning.get("checkDollars"), "number")
            )

        for earning in employee_check_details.get("directDeposits") or []:
            self.add_query(employee, employee_check_details, system_id, employee_id, "Direct Deposits", 
                self.validate(earning.get("itemDescription")), 
                0.0,
                self.validate(earning.get("depositAmount"), "number")
            )

        for earning in employee_check_details.get("taxes") or []:
            self.add_query(employee, employee_check_details, system_id, employee_id, "Taxes", 
                self.validate(earning.get("itemCode") or earning.get("itemDescription")), 
                self.validate(earning.get("checkHours"), "number"),
                self.validate(earning.get("checkDollars"), "number")
            )

        for earning in employee_check_details.get("earnings") or []:
            self.add_query(employee, employee_check_details, system_id, employee_id, "Earning", 
                self.validate(earning.get("itemCode") or earning.get("itemDescription")), 
                self.validate(earning.get("checkHours"), "number"),
                self.validate(earning.get("checkDollars"), "number")
            )
            
        self.add_query(employee, employee_check_details, system_id, employee_id, "NetPay", 
            "", 
            0.0,
            self.validate(employee_check_details.get("netPay"), "number")
        )

    def add_query(self, employee, employee_check_details, system_id, employee_id, earning_group, earning_code, hours, dallers):
        today = date.today().strftime('%Y-%m-%d')
        try:
            self.cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM employee_checks WHERE system_id='{system_id}' and earning_code='{earning_code}' and earning_group='{earning_group}')
                INSERT employee_checks (
                    facility_name, department, department_code, employee_first_name, employee_last_name, 
                    position, position_code, system_id, employee_id, hours, dollars, earning_code,
                    earning_group, check_date, period_end_date, check_type, check_number, load_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                self.validate(employee_check_details.get("legalCompanyName")),
                self.validate(employee_check_details.get("Department", {}).get("description")),
                self.validate(employee_check_details.get("Department", {}).get("code")),
                self.validate(employee_check_details.get("employeeName")).split(" ")[0],
                self.validate(employee_check_details.get("employeeName")).split(" ")[-1],
                self.validate(employee_check_details.get("Position", {}).get("description")),
                self.validate(employee_check_details.get("Position", {}).get("code")),
                system_id,
                employee_id,
                hours,
                dallers,
                earning_code,
                earning_group,
                self.validate(employee_check_details.get("checkDate"), "datetime"),
                self.validate(employee_check_details.get("periodEndDate"), "datetime"),
                self.validate(employee_check_details.get("checkTypeDescription")),
                self.validate(employee_check_details.get("checkNumber")),
                today
            )
            self.conn.commit()
            if self.count / 100 == 0:
                logging.info(f"counter: {self.count} | legal_code: {employee.get('legalCode')} | facility_name: {employee_check_details.get('legalCompanyName')}")
            self.count += 1
        except Exception as e:
            logging.exception(f"insert_employee_checks: {e}")
            time.sleep(60)
            logging.exception(f"insert_employee_checks: connecting database again")
            self.connect_database()
            self.add_query(employee, employee_check_details, system_id, employee_id, earning_group, earning_code, hours, dallers)


    def validate(self, item, field_type="string"):
        if item == None:
            item = ""
            if field_type == "number":
                item = 0.0
            if field_type == "datetime":
                item = None
        if type(item) == str:
            item = item.replace("'", "`")
        if field_type == "datetime" and item is not None:
            item = item.split('T')[0]
        return item


    def get_employee_status_code(self, status):
        employment_status_types = {
            "Active": "A",
            "Inactive": "I",
            "Terminated": "T"
        }
        if status in employment_status_types:
            return employment_status_types[status]
        else:
            return status

    # Create csv file
    def get_writer(self):
        output_file = open(f'myisolved_{self.name}.csv', mode='w', newline='')
        output_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        csv_headers = {
            "details": [
                "Facility Name", "Department", "Department Code", "Employee First Name", "Employee Middle Name", 
                "Employee Last Name", "Hire Date", "Rehire Date", "Termination Date", "Leave Date", 
                "Seniority Date", "Position", "Position ID", "Employee ID", "Status", "Status Type",
                "Email", "Pay Type", "Hourly Rate",
            ],
            "checks": [
                "Facility Name", "Department", "Department Code", "Employee First Name", "Employee Last Name",
                "Position", "Position Code", "Employee ID", "Hours", "Dollars", "Earning Code",
                "Earning Group", "Check Date", "Period End Date", "Check Type", "Check Number",
            ]
        }
        output_writer.writerow(csv_headers[self.name])
        return output_writer

    
    def write(self, values):
        row = []
        for header in self.csv_headers:
            row.append(values.get(header, ''))
        self.csv_writer.writerow(row)


    def setup_log(self):
        filename = 'history.log'
        if not os.path.isdir("logs"):
            os.makedirs("logs")
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
        logging.basicConfig(
            filename=f"logs/{self.name}-{timestamp}.log",
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S')
        logging.info("==============================================================")
        logging.info("========================= start ==============================")
        logging.info("==============================================================")


if __name__ == "__main__":
    Main()
