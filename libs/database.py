from datetime import date
from datetime import datetime

import mariadb
import pyodbc

from libs import configuration


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def create_sql(table, data):
    return "INSERT INTO " + table + " (" + ", ".join(data.keys()) + ") VALUES (" + ", ".join(
        ["?" for k in data]) + ");"


# --[ MariaDB ]---------------------------------------------------------------------------------------------------------
def mariadb_acquire_connect():
    if not configuration.settings['databases']['mariadb']['pool']:
        conn = mariadb.connect(**configuration.settings['databases']['mariadb']['connection'])
        configuration.settings['databases']['mariadb']['pool'] = conn
    return configuration.settings['databases']['mariadb']['pool']


def mariadb_close_connect():
    conn = mariadb_acquire_connect()
    conn.close()


def mariadb_commit():
    conn = mariadb_acquire_connect()
    conn.commit()


def mariadb_execute_sql(sql, record, fetch_one=None, fetch_all=None):
    if configuration.settings['debug']['sql']:
        print("Execute sql: \n\t%s\n\t%s" % (sql, record))

    cursor = mariadb_acquire_connect().cursor()
    cursor.execute(sql, record)
    if fetch_one:
        return cursor.fetchone()
    if fetch_all:
        return cursor.fetchall()


def mariadb_exist(table, column, value):
    sql = 'SELECT TRUE AS exist FROM ' + table + ' WHERE ' + column + '=?'
    if mariadb_execute_sql(sql, (value,), fetch_one=True):
        return True
    else:
        return False


def mariadb_insert(table, record):
    sql = create_sql(table, record)
    mariadb_execute_sql(sql, list(record.values()))
    mariadb_commit()


# --[ MS Access ]-------------------------------------------------------------------------------------------------------
def access_acquire_connect():
    if not configuration.settings['databases']['access']['pool']:
        conn = pyodbc.connect(configuration.settings['databases']['access']['connection'])
        configuration.settings['databases']['access']['pool'] = conn.cursor()
    return configuration.settings['databases']['access']['pool']
