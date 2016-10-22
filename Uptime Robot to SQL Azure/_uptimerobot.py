""" _uptimerobot.py
 
 Getting monitors' datat using Uptime Robot's API and storing in a database
 
"""
import json
import collections
import os
 
import requests
 
import pyodbc
 
MONITOR_DATA = []
LOG_DATA = []
TIME_DATA = []
DB_CONNECTION_STRING = {"driver":'<driver>', "server": '<server>',
                        "user": '<username>', "password": '<password>',
                        "database": '<database name>',
                        "port":'1433', "TDS" :'8.0'}
API_INFO = {"monitor_apikey":'<API KEY>'}

 
def get_monitors(apikey):
     """
     Returns status and response payload for all known monitors.
 
     """
     formatted = "&format=json&nojsoncallback=1&responseTimes=1&logs=1"
     url = "https://api.uptimerobot.com/"
     url = "getMonitors?apiKey=" +  str(apikey) + formatted
     info = requests.get(url)
 
     return info
 
def create_tables(db_connection):
     """
     Creates all of the tables in the database if it does not exists.
 
     """
     cursor = db_connection.cursor()
 
     try:
 
         monitor_init = """
             IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MONITOR' AND xtype ='U')
                     CREATE TABLE MONITOR (
                     monitor_id int NOT NULL,
                     friendlyname VARCHAR(20),
                     url VARCHAR(100),
                     type INT,
                     interval INT,
                     status  CHAR(20),
                     uptimeratio FLOAT,
                     PRIMARY KEY (monitor_id))
                     
                     """
 
         cursor.execute(monitor_init)
         log_init = """
                 IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MONITOR_LOG' AND xtype ='U')
                     CREATE TABLE MONITOR_LOG (
                     monitor_id int NOT NULL,
                     date_time DATETIME,
                     type INT,
                     PRIMARY KEY (monitor_id, date_time),
                     FOREIGN KEY (monitor_id) REFERENCES MONITOR(monitor_id))
                         
                     """
         cursor.execute(log_init)
 
         time_init = """
             IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MONITOR_TIME' AND xtype ='U')
                     CREATE TABLE MONITOR_TIME (
                     monitor_id int NOT NULL,
                     date_time DATETIME,
                     type INT,
                     PRIMARY KEY (monitor_id, date_time),
                     FOREIGN KEY (monitor_id) REFERENCES MONITOR(monitor_id) )
                         
                     """
         cursor.execute(time_init)
         cursor.commit()
 
     except AttributeError:
         print("tables could not be created")
 
def retrieve_logs(id_sql, logs):
     """
     Grabs log data from monitor and stores in lists
 
     """

     for log in logs:
         log_type = log["type"]
         log_date = log["datetime"]
         LOG_DATA.append([log_date, id_sql, log_date, log_type])
 
     return LOG_DATA
 
def retrieve_times(id_sql, responsetimes):
     """
     Grabs response time data from monitor and stores in lists
 
     """

     for time in responsetimes:
         time_date = time["datetime"]
         time_value = time["value"]
         TIME_DATA.append([time_date, id_sql, time_date, time_value])
 
     return TIME_DATA
 
def retrieve_data():
     """
     Grabs data from uptimerobot using the api key, seperates the data
     and stores it in three seperate lists.
 
     """

     data = json.loads((((get_monitors(API_INFO["monitor_apikey"]).content).decode("utf-8")))
     data = data["monitors"]["monitor"]
 

     for monitor in data:
 
         id_sql = monitor["id"]
         fn_sql = monitor["friendlyname"]
         url_sql = monitor["url"]
         type_sql = monitor["type"]
         interval_sql = monitor["interval"]
         status_sql = monitor["status"]
         ratio_sql = monitor["alltimeuptimeratio"]
         logs = monitor["log"]
         responsetimes = monitor["responsetime"]
 
         MONITOR_DATA.append([id_sql, id_sql, fn_sql, url_sql,
                              type_sql, interval_sql, status_sql, ratio_sql])
 
         retrieve_logs(id_sql, logs)
 
         retrieve_times(id_sql, responsetimes)
 
     return MONITOR_DATA, LOG_DATA, TIME_DATA
 
def insert_data(db_connection):
     """
     Inserts data from the lists created and into SQL Azure
 
     """
 
     cursor = db_connection.cursor()
 
     insert_monitor = """
                     BEGIN
                         IF NOT EXISTS (SELECT * FROM MONITOR
                             WHERE monitor_id=?)
                         BEGIN
                             INSERT INTO MONITOR
                             VALUES (?,?,?,?,?,?,?)
                         END
                     END
                 
                     """
     insert_log = """
                     BEGIN
                         IF NOT EXISTS (SELECT * FROM MONITOR_LOG
                             WHERE date_time=?)
                         BEGIN
                             INSERT INTO MONITOR_LOG
                             VALUES (?,?,?)
                         END
                     END
                 
                     """
     insert_time = """
                     BEGIN
                         IF NOT EXISTS (SELECT * FROM MONITOR_TIME
                             WHERE date_time=?)
                         BEGIN
                             INSERT INTO MONITOR_TIME
                             VALUES (?,?,?)
                         END
                     END
                  """
 
     cursor.executemany(insert_monitor, MONITOR_DATA)
     cursor.commit()
     cursor.executemany(insert_log, LOG_DATA)
     cursor.commit()
     cursor.executemany(insert_time, TIME_DATA)
     cursor.commit()
 
def main():
     """
     The main logic flow of the script
 
     """
     db_connection = pyodbc.connect(driver=DB_CONNECTION_STRING["driver"], server=DB_CONNECTION_STRING["server"],
                                    user=DB_CONNECTION_STRING["user"], password=DB_CONNECTION_STRING["password"],
                                    database=DB_CONNECTION_STRING["database"],
                                    port=DB_CONNECTION_STRING["port"],
                                    TDS_Version=DB_CONNECTION_STRING["TDS"])
     
     create_tables(db_connection)
     retrieve_data()
     insert_data(db_connection)
     db_connection.commit()
     db_connection.close()
 
# Execute Main
if __name__ == "__main__":
     main()