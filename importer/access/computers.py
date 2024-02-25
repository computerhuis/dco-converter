import traceback
from json import dumps

from importer.access.individuals import import_individual
from libs import configuration
from libs import database


def export_access_import_mariadb():
    if configuration.settings['databases']['access']:
        cursor = database.access_acquire_connect()
        cursor.execute('SELECT Tbl_computers.* FROM Tbl_computers WHERE [Datum Gift] > #' + configuration.settings[
            'import_date_from'] + '#')
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            __import_computer(dict(zip(columns, row)))


def import_computer(id):
    print("Lookup computer with id: " + str(id))
    if id is not None:
        if configuration.settings['databases']['access']:
            cursor = database.access_acquire_connect()
            cursor.execute('SELECT Tbl_computers.* FROM Tbl_computers WHERE Computernummer=' + str(id))
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                __import_computer(dict(zip(columns, row)))


def __import_computer(dbrow):
    if dbrow is not None:
        if not database.mariadb_exist('computers', 'id', dbrow['Computernummer']):
            print("Import computer with id: " + str(dbrow['Computernummer']))
            pc_specificatie = {
                "processor": str(dbrow['Processor']).strip(),
                "geheugen": str(dbrow['Processor']).strip(),
                "harddisk": str(dbrow['HDD SSD']).strip(),
                "optisch": str(dbrow['Optische apparaten']).strip(),
                "cardreader": dbrow['Cardreader'],
                "videokaart": str(dbrow['Videokaart']).strip(),
                "overige": str(dbrow['overige ingebouwde apparaten']).strip(),
                "bijzonderheden": str(dbrow['Bijzonderheden']).strip(),
                "software": str(dbrow['Software']).strip()
            }

            specificatie = {x: y for x, y in pc_specificatie.items() if
                            (y is not None and y != '' and y != 'None'
                             and str(y).upper() != 'GEEN' and y != 'Onboard' and y is not False)}

            type_kast = str(dbrow['Type kast']).strip().upper()
            if type_kast == 'LAPTOP' or type_kast == 'LAPTUP' or type_kast.startswith('NOTEBOOK'):
                type_kast = 'LAPTOP'
            else:
                type_kast = 'DESKTOP'

            status = str(dbrow['Status']).strip().upper()
            match status:
                case "BINNENGEKOMEN GIFT":
                    status = "INCOMING_GIFT"
                case "GESCHIKT VOOR VERKOOP":
                    status = "SUITABLE_FOR_GIFT"
                case "KLAAR VOOR VERKOOP":
                    status = "RESERVED"
                case "KLANT PC":
                    status = "CUSTOMER_PC"
                case "VERKOCHT":
                    status = "GIFT_ISSUED"
                case "SLOOP":
                    status = "DEMOLITION"
                case _:
                    status = None

            fabrikant = dbrow['Fabrikant']
            if fabrikant is None:
                fabrikant = "Onbekend"

            computer = {'id': dbrow['Computernummer'],
                        'form_factor': type_kast,
                        'manufacturer': fabrikant,
                        'model': dbrow['Model nummer'],
                        'specification': dumps(specificatie),
                        'customer_id': dbrow['gebruikersnummer'],
                        'status': status
                        }

            if not dbrow['Datum Gift']:
                computer['registered'] = configuration.settings['import_date_from']
            else:
                computer['registered'] = dbrow['Datum Gift']

            try:
                import_individual(dbrow['gebruikersnummer'])
                database.mariadb_insert('computers', computer)
                database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(computer, default=database.json_serial))
                traceback.print_exc()
                exit(1)
