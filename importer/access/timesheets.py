import traceback
from json import dumps

from importer.access.individuals import import_individual
from libs import configuration
from libs import database
from libs.database import mariadb_execute_sql


def export_access_import_mariadb():
    if configuration.settings['databases']['access']:
        cursor = database.access_acquire_connect()
        cursor.execute(
            'SELECT * FROM Tbl_Tijdsregistratie WHERE Datum > #' + configuration.settings[
                'import_date_from'] + '# and Activiteit is not null')
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            __import_timesheet(dict(zip(columns, row)))


def __exist_timesheet(person_id, activity_id, registered):
    sql = 'SELECT TRUE AS exist FROM timesheets WHERE person_id=? AND activity_id=? AND registered=?'
    if mariadb_execute_sql(sql, (person_id, activity_id, registered), fetch_one=True):
        return True
    else:
        return False


def __import_timesheet(dbrow):
    activiteit = {
        'CURSUSSEN': 1,
        'LESOPMAAT': 2,
        'MEDEWERKER': 3,
        'MEDEWERKER WERKPLAATS': 4,
        'ONLINE BEGELEIDING': 5,
        'TAALONDERSTEUNING': 6,
        'VRIJE INLOOP': 7,
        'WERKPLAATS': 8,
        'WORKSHOPS': 9,
        'VLINDERTUIN': 10
    }

    if str(dbrow['Activiteit']).upper() in activiteit:
        activiteit_nr = activiteit[str(dbrow['Activiteit']).upper()]
        tijdregistratie = {'person_id': dbrow['Gebruikersnummer'],
                           'registered': dbrow['Datum'].date(),
                           'activity_id': activiteit_nr,
                           'unregistered': dbrow['Datum'].date()}
        try:
            if not __exist_timesheet(tijdregistratie['person_id'],
                                     tijdregistratie['activity_id'],
                                     tijdregistratie['registered']):
                import_individual(dbrow['Gebruikersnummer'])
                database.mariadb_insert('timesheets', tijdregistratie)
                database.mariadb_commit()
        except:
            print("INSERT ERROR: " + dumps(tijdregistratie, default=database.json_serial))
            traceback.print_exc()
            exit(1)
    else:
        print("Not supported: " + str(dbrow['Activiteit']))
        exit(1)
