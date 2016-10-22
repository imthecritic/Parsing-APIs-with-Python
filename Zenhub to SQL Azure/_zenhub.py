""" zenhub.py
    parses both issues and board responses from Zenhub's API
"""
import os
import collections

import json


import pyodbc

ISSUE_DATA = []
BOARD_DATA = []
DB_CONNECTION_STRING = {"driver":'<driver>', "server": '<server>',
                        "user": '<username>', "password": '<password>',
                        "database": '<database name>',
                        "port":'1433', "TDS" :'8.0'}



def create_tables(connection):
    """
    Creates issue and board tables if they do not exists.
    """

    cursor = connection.cursor()

    try:
        cursor.execute("""
                     IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ISSUE' AND xtype ='U')
                        CREATE TABLE ISSUE (
                        user_id VARCHAR(100) NOT NULL,
                        issue_type VARCHAR (100),
                        date DATETIME,
                        to_estimate VARCHAR (100),
                        from_estimate VARCHAR (100),
                        from_pipeline VARCHAR (100),
                        to_pipeline VARCHAR (100),
                        PRIMARY KEY (issue_type, date)
                        )""")

        cursor.commit()

        cursor.execute("""
                        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='BOARD' AND xtype ='U')
                        CREATE TABLE BOARD (
                        category VARCHAR (25),
                        issue_number INTEGER,
                        position INTEGER,
                        estimate INTEGER,
                        PRIMARY KEY (category, issue_number)
                       )""")

        cursor.commit()

    except KeyError:
        print("tables could not be created")


def retrieve_issue_data(issue_response):

    """
    Organizes and stores the issues response in a list to be prepared to be
    inserted in the database
    """
    for data in issue_response:
        user_id = data["user_id"]
        issue_type = data["type"]
        date = data["created_at"]
        if "to_estimate" in data.keys():
            to_estimate = data["to_estimate"]["value"]
        else:
            to_estimate = None
        if "from_estimate" in data.keys():
            from_estimate = data["from_estimate"]["value"]
        else:
            from_estimate = None
        if "from_pipeline" in data.keys():
            from_pipeline = data["from_pipeline"]["name"]
        else:
            from_pipeline = None
        if "to_pipeline" in data.keys():
            to_pipeline = data["to_pipeline"]["name"]
        else:
            to_pipeline = None

        ISSUE_DATA.append([date, issue_type, user_id, issue_type, date, to_estimate,
                           from_estimate, from_pipeline, to_pipeline])
    return ISSUE_DATA

def retrieve_board_data(board_response):
    """
    Organizes and stores the board response in a list to be prepared to be
    inserted in the database
    """
    pipelines = board_response["pipelines"]
    for pipeline in pipelines:
        name = pipeline["name"]
        issues = pipeline["issues"]
        for issue in issues:
            issue_number = issue["issue_number"]
            if "position" in issue.keys():
                position = issue["position"]
            else:
                position = None
            if "estimate" in issue.keys():
                estimate = issue["estimate"]["value"]
            else:
                estimate = None
            BOARD_DATA.append([issue_number, name, name, issue_number, position, estimate])

    return BOARD_DATA


def insert_issue_data(connection):
    """
    Inserts data from the issue list created and into SQL Azure
    """
    cursor = connection.cursor()

    issue_insert = """
                    BEGIN
                        IF NOT EXISTS (SELECT * FROM  ISSUE
                            WHERE date=? and issue_type=?)
                        BEGIN
                            INSERT INTO  ISSUE
                            VALUES (?,?,?,?,?,?,?)
                        END
                    END
                    """
    cursor.executemany(issue_insert, ISSUE_DATA)
    cursor.commit()

def insert_board_data(connection):
    """
    Inserts data from the board list created and into SQL Azure
    """
    cursor = connection.cursor()

    board_insert = """
                    BEGIN
                        IF NOT EXISTS (SELECT * FROM  BOARD
                            WHERE issue_number=? and category=?)
                        BEGIN
                            INSERT INTO BOARD
                            VALUES (?,?,?,?)
                        END
                    END
                    """

    cursor.executemany(board_insert, BOARD_DATA)
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

    with open('zenhubissue.json') as data_file:
        issue_response = json.load(data_file)

    with open('zenhubboard.json') as data_file2:
        board_response = json.load(data_file2)

    create_tables(db_connection)
    retrieve_issue_data(issue_response)
    insert_issue_data(db_connection)
    retrieve_board_data(board_response)
    insert_board_data(db_connection)

    db_connection.commit()
    db_connection.close()

# Execute Main
if __name__ == "__main__":
    main()