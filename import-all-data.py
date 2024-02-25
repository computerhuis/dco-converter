import time
import traceback
from datetime import timedelta

from importer import dm, gwbp
from importer.access import computers, tickets, individuals, timesheets
from libs import configuration, database

if __name__ == "__main__":
    result = {}
    start_time = time.time()
    print('Access - import data ')
    try:
        configuration.init()
        if configuration.settings:
            # --[ CBS DATA ]--------------------------------------------------------------------------------------------
            gwbp.import_postal_codes('data/postals.json')

            # --[ DATA MIGRATION PRE-LOAD ]-----------------------------------------------------------------------------
            dm.import_activities('data/activities.csv')
            dm.import_individuals('data/individuals.csv')
            dm.import_individual_login('data/individual_login.csv')
            dm.import_individual_authorities('data/individual_authorities.csv')

            # --[ ACCESS DATA ]-----------------------------------------------------------------------------------------
            if configuration.settings['databases']['access']:
                computers.export_access_import_mariadb()
                individuals.export_access_import_mariadb()
                timesheets.export_access_import_mariadb()
                tickets.export_access_import_mariadb()
        database.mariadb_close_connect()
    except:
        traceback.print_exc()
    print("--- %s duration ---" % timedelta(seconds=round(time.time() - start_time, 2)))
