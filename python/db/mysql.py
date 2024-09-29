# -*- coding: utf-8 -*-
# !/usr/bin/env python3
# ================================================================================================
# Module: readers.py
# Author: Fernando Theodoro Guimarães
# E-mail:
# Description: This module is responsible for methods that apply operations to Pandas DataFrames.
# ================================================================================================

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import MySQLConnection
from mysql.connector import MySQLCursor


class MySqlConnector:
    def __init__(self, config: dict) -> None:
        self.config = config

    def open_connection(self) -> MySQLConnection | None:
        try:
            cnx = mysql.connector.connect(**self.config)
            if cnx and cnx.is_connected():
                return cnx
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(e)

    def create_cursor(self, cnx: MySQLConnection) -> MySQLCursor:
        return cnx.cursor()

    def commit_transaction(self, cnx: MySQLConnection) -> None:
        cnx.commit()

    def close_obj(self, mysql_obj: MySQLConnection | MySQLCursor) -> None:
        mysql_obj.close()


