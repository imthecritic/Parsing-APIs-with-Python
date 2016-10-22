"""
okta.py
Grabs event data from api and parses it and stores it in table according to type.
"""

import os
import collections

import json
import pyodbc



ACTORS_DATA = []
TARGET_DATA = []
STANDARD_DATA = []
ACTION_DATA = []
DB_CONNECTION_STRING = {"driver":'<driver>', "server": '<server>',
                        "user": '<username>', "password": '<password>',
                        "database": '<database name>',
                        "port":'1433', "TDS" :'8.0'}

def create_tables(connection):
    """
    Creates event, action, actor, and target tables if they do not exists.
    """

    cursor = connection.cursor()

    try:
        cursor.execute("""
                     IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EVENT' AND xtype ='U')
                        CREATE TABLE EVENT (
                        event_id VARCHAR(100) NOT NULL,
                        published_id VARCHAR (100),
                        request_id VARCHAR(100),
                        session_id VARCHAR (100),
                        payload NVARCHAR (MAX),
                        PRIMARY KEY (event_id)
                        )""")

        cursor.commit()

        cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EVENT_ACTION' AND xtype ='U')
                    CREATE TABLE EVENT_ACTION (
                    event_id VARCHAR(100),
                    message VARCHAR (100),
                    categories NVARCHAR(MAX),
                    object_type VARCHAR(100),
                    request_uri VARCHAR(100),
                    FOREIGN KEY(event_id) REFERENCES EVENT(event_id)
                    )  """)

        cursor.commit()

        cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EVENT_ACTOR' AND xtype ='U')
                    CREATE TABLE EVENT_ACTOR (
                    event_id VARCHAR(100),
                    actor_id VARCHAR (MAX),
                    actor_display VARCHAR(100),
                    actor_object_type VARCHAR(100),
                    actor_login VARCHAR(100),
                    actor_ip VARCHAR(100),
                    FOREIGN KEY(event_id) REFERENCES EVENT(event_id)
                    )""")

        cursor.commit()

        cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EVENT_TARGET' AND xtype ='U')
                    CREATE TABLE EVENT_TARGET
                    (
                    event_id VARCHAR(100),
                    target_id VARCHAR(100),
                    target_display VARCHAR(100),
                    target_login VARCHAR(100),
                    target_object_type VARCHAR(100),
                    FOREIGN KEY(event_id) REFERENCES EVENT(event_id)
                    ) """)

        cursor.commit()

    except KeyError:
        print("tables could not be created")

def retrieve_action_data(eventid, data):
    """
    Grabs action data and eventid returned from the standard data, flattens it, and stores in a list
    """
    action = data["action"]
    action_categories = json.dumps(action["categories"])
    action_message = action["message"]
    action_object_type = action["objectType"]
    action_request_uri = action["requestUri"]
    ACTION_DATA.append([action_categories, eventid, eventid,
                        action_message, action_object_type, action_request_uri])

    return ACTION_DATA

def retrieve_actor_data(eventid, data):
    """
    Grabs actor data and eventid returned from the standard data, flattens it, and stores in a list
    """
    actors = data["actors"]
    for item in actors:
        actor_id = item["id"]
        actor_display = item["displayName"]
        actor_object_type = item["objectType"]
        if "login" in item.keys():
            actor_login = item["login"]
        else:
            actor_login = None
        if "ipAddress" in item.keys():
            actor_ip = item["ipAddress"]
        else:
            actor_ip = None
        ACTORS_DATA.append([actor_id, eventid, actor_id, actor_display,
                            actor_object_type, actor_login, actor_ip])

    return ACTORS_DATA

def retrieve_target_data(eventid, data):
    """
    Grabs target data and eventid returned from the standard data, flattens it, and stores in a list
    """
    targets = data["targets"]
    for item in targets:
        target_id = item["id"]
        target_display = item["displayName"]
        target_object_type = item["objectType"]
        if "login" in item.keys():
            target_login = item["login"]
        else:
            target_login = None
        TARGET_DATA.append([target_id, eventid, target_id,
                            target_display, target_login, target_object_type])

    return TARGET_DATA

def retrieve_standard_data(response):
    """
    Loops through the returned json , flattens it, and stores in a list also calls
    the functions retrieve_target_data, retrieve_actor_data, retrieve_action_data
    """

    for data in response:
        eventid = data["eventId"]
        published = data["published"]
        if "requestId" in data.keys():
            requestid = data["requestId"]
        else:
            requestid = None
        if "sessionId" in data.keys():
            sessionid = data["sessionId"]
        else:
            sessionid = None
        payload = json.dumps(data)
        STANDARD_DATA.append([payload, eventid, eventid, published, requestid, sessionid])


        retrieve_action_data(eventid, data)

        retrieve_actor_data(eventid, data)

        retrieve_target_data(eventid, data)

    return STANDARD_DATA

def insert_data(connection):
    """
    Inserts data from the lists created and into SQL Azure
    """
    cursor = connection.cursor()

    event_insert = """
                    DECLARE @json NVARCHAR(max)
                        SET @json = ?
                        BEGIN
                            IF NOT EXISTS (SELECT * FROM  EVENT
                                WHERE event_id=?)
                            BEGIN
                                INSERT INTO EVENT
                                VALUES(
                                    ?,
                                    ?,
                                    ?,
                                    ?,
                                    JSON_QUERY (@json, '$')
                                );
                            END
                        END
    
                        
                    """

    action_insert = """
                        DECLARE @json NVARCHAR(max)
                        SET @json = ?
                        BEGIN
                            IF NOT EXISTS (SELECT * FROM  EVENT_ACTION
                                WHERE event_id=?)
                            BEGIN
                                INSERT INTO EVENT_ACTION
                                VALUES(
                                    ?,
                                    ?,
                                    JSON_QUERY (@json, '$'),
                                    ?,
                                    ?
                                );
                            END
                        END
                    """

    actor_insert = """
                    BEGIN
                        IF NOT EXISTS (SELECT * FROM  EVENT_ACTOR
                            WHERE actor_id=?)
                        BEGIN
                            INSERT INTO  EVENT_ACTOR
                            VALUES (?,?,?,?,?,?)
                        END
                    END
                    """

    target_insert = """
                    BEGIN
                        IF NOT EXISTS (SELECT * FROM  EVENT_TARGET
                            WHERE target_id=?)
                        BEGIN
                            INSERT INTO  EVENT_TARGET
                            VALUES (?,?,?,?,?)
                        END
                    END
                    """

    cursor.executemany(event_insert, STANDARD_DATA)
    cursor.commit()
    cursor.executemany(action_insert, ACTION_DATA)
    cursor.commit()
    cursor.executemany(actor_insert, ACTORS_DATA)
    cursor.commit()
    cursor.executemany(target_insert, TARGET_DATA)
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

    with open('example.json') as data_file:
        response = json.load(data_file)

    create_tables(db_connection)
    retrieve_standard_data(response)
    insert_data(db_connection)
    db_connection.commit()
    db_connection.close()

# Execute Main
if __name__ == "__main__":
    main()