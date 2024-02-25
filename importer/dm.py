import csv
import traceback
from json import dumps

from libs import database


def import_activities(filename_in):
    print('Loading csv file: ' + filename_in)
    with open(filename_in, mode='r', encoding='utf8') as csv_file:
        for line in csv.DictReader(csv_file, delimiter=';'):
            try:
                if not database.mariadb_exist('activities', 'id', line['id']):
                    database.mariadb_insert('activities', line)
                    database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(line, default=database.json_serial))
                traceback.print_exc()
        csv_file.close()


def import_individuals(filename_in):
    print('Loading csv file: ' + filename_in)
    with open(filename_in, mode='r', encoding='utf8') as csv_file:
        for line in csv.DictReader(csv_file, delimiter=';'):
            row = {}
            try:
                for key, value in line.items():
                    if value and value.strip() != '':
                        row[key] = value

                if not database.mariadb_exist('individuals', 'id', row['id']):
                    database.mariadb_insert('individuals', row)
                    database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(row, default=database.json_serial))
                traceback.print_exc()
        csv_file.close()


def import_individual_login(filename_in):
    print('Loading csv file: ' + filename_in)
    with open(filename_in, mode='r', encoding='utf8') as csv_file:
        for line in csv.DictReader(csv_file, delimiter=';'):
            row = {}
            try:
                for key, value in line.items():
                    if value and value.strip() != '':
                        row[key] = value

                if not database.mariadb_exist('individual_login', 'username', row['username']):
                    database.mariadb_insert('individual_login', row)
                    database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(row, default=database.json_serial))
                traceback.print_exc()
        csv_file.close()


def import_individual_authorities(filename_in):
    print('Loading csv file: ' + filename_in)
    with open(filename_in, mode='r', encoding='utf8') as csv_file:
        for line in csv.DictReader(csv_file, delimiter=';'):
            row = {}
            try:
                for key, value in line.items():
                    if value and value.strip() != '':
                        row[key] = value

                if not database.mariadb_exist('individual_authorities', 'username', row['username']):
                    database.mariadb_insert('individual_authorities', row)
                    database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(row, default=database.json_serial))
                traceback.print_exc()
        csv_file.close()
