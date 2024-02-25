import traceback
from json import dumps

from importer.access.computers import import_computer
from libs import configuration
from libs import database


def export_access_import_mariadb():
    if configuration.settings['databases']['access']:
        cursor = database.access_acquire_connect()
        cursor.execute('select * from Tbl_Reparaties_main WHERE [datum inname] > #' + configuration.settings[
            'import_date_from'] + '#')
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            __import_ticket(dict(zip(columns, row)))


def import_ticket(id):
    print("Lookup ticket with id: " + str(id))
    if id is not None:
        if configuration.settings['databases']['access']:
            cursor = database.access_acquire_connect()
            cursor.execute('SELECT Tbl_Reparaties_main.* FROM Tbl_Reparaties_main WHERE Reparatienummer=' + str(id))
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                __import_ticket(dict(zip(columns, row)))


def __import_ticket(dbrow):
    if dbrow is not None:
        if not database.mariadb_exist('tickets', 'id', dbrow['Reparatienummer']):
            print("Import ticket with id: " + str(dbrow['Reparatienummer']))
            ticket_type = 'REPAIR'
            if 'uitgifte' in str(dbrow['Probleem']).lower():
                ticket_type = 'ISSUE'

            ticket = {
                'id': dbrow['Reparatienummer'],
                'ticket_type': str(ticket_type).upper().strip().replace(' ', '_'),
                'registered': dbrow['datum inname'],
                'computer_id': dbrow['Computernummer'],
                'description': dumps({
                    'probleem': dbrow['Probleem'],
                    'backup': dbrow['Backup'],
                    'meegeleverde_accessoires': dbrow['bijgeleverde accessoires'],
                    'samenvatting': dbrow['Samenvatting Reparatie']
                })
            }

            try:
                import_computer(dbrow['Computernummer'])
                database.mariadb_insert('tickets', ticket)
                database.mariadb_commit()

                __import_ticket_status_and_log(dbrow)
                __import_ticket_logs(dbrow['Reparatienummer'])
                database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(ticket, default=database.json_serial))
                traceback.print_exc()
                exit(1)


def __lookup_volunteer_id(name):
    if not name:
        return 1

    lookup_name = str(name).strip()
    person = {
        #  Joris Pierre Kleijnen
        'joris': 1937,
        'Joris': 1937,

        # Frans van der Meijden
        'Frans': 1544,
        'frans': 1544,
        'Frans/Ali': 1544,
        'Frans/Jan': 1544,
        'Frans/Antonie': 1544,
        'Frans/Ary': 1544,
        'Frans/Sjef': 1544,

        # Ary Safari - Al Baldawi
        'Ali': 1648,
        'Ali & Stephan': 1648,
        'Ali en Bas': 1648,
        'Ali/Frans': 1648,
        'Ali/Jan': 1648,
        'Ali/Stephan': 1648,
        'joris en ali': 1648,
        'Ale': 1648,
        'adi': 1648,
        'Ary': 1648,
        'Ari': 1648,
        'Ary/Frans': 1648,
        'Ary/Antonie': 1648,
        'ary': 1648,
        'Adi': 1648,

        # Henri Dona
        'henri': 1804,
        'Hen ri': 1804,
        'Henry': 1804,
        'Henri': 1804,

        # Peter Ruyters
        'Peter': 740,

        # Sjef Lievens
        'Sjef': 897,
        'swjef': 897,
        'sjef': 897,
        'Sjef/Frans': 897,
        'Sjef en Frans': 897,
        'sjf': 897,

        # Thomas Carpentier
        'Thomas': 1839,
        'Thomas & Frans': 1839,
        'Thomas/Frans': 1839,

        # Jan van der Pol
        'Jan': 1408,
        'Jan/Frans': 1408,
        'JAN/Frans': 1408,
        'Jan van de Pol': 1408,

        # Tim Voogt
        'Tim Voogt': 2097,
        'Tim': 2097,

        # Sander Stumpel
        'Sander': 2106,
        'sander': 2106,

        # Willie Voets
        'Willie/Frans': 2174,
        'Willie-Frans': 2174,
        'Wil-Frans': 2174,
        'Willie': 2174,
        'Willie Voets': 2174,
        'willie': 2174,

        # Wil Verberne
        'Wil': 1284,

        # Antonie Gelderblom
        'Antonie': 2268,
        'Antonie/Frans': 2268,
    }

    if lookup_name not in person:
        print(lookup_name)
        exit(1)

    return person[lookup_name]


def __import_ticket_status_and_log(dbrow):
    if dbrow is not None:
        person_id = __lookup_volunteer_id(dbrow['aangenomen door'])
        uitgevoerd_door = __lookup_volunteer_id(dbrow['Medewerker'])

        database.mariadb_insert('ticket_status', {
            'ticket_id': dbrow['Reparatienummer'],
            'date': dbrow['datum inname'],
            'volunteer_id': person_id,
            'status': 'OPEN'})

        # --[ READY ]----------------------------------------------------------------------------------------------------
        if 'Datum opgelost' in dbrow and dbrow['Datum opgelost'] is not None:
            database.mariadb_insert('ticket_status', {
                'ticket_id': dbrow['Reparatienummer'],
                'date': str(dbrow['Datum opgelost'])[:10] + ' 01:00:00',
                'volunteer_id': uitgevoerd_door,
                'status': 'READY'})

        # --[ CUSTOMER_CALLED ]---------------------------------------------------------------------------------------------
        ready = False
        if 'Datum Gebeld3' in dbrow and dbrow['Datum Gebeld3'] is not None:
            ready = True

            database.mariadb_insert('ticket_status', {
                'ticket_id': dbrow['Reparatienummer'],
                'date': str(dbrow['Datum Gebeld3'])[:10] + ' 18:00:00',
                'volunteer_id': uitgevoerd_door,
                'status': 'CUSTOMER_INFORMED'})

            database.mariadb_insert('ticket_log', {
                'ticket_id': dbrow['Reparatienummer'],
                'date': str(dbrow['Datum Gebeld3'])[:10] + ' 18:00:00',
                'volunteer_id': uitgevoerd_door,
                'log': dbrow['Reactie Gebeld3']
            })

        if 'Datum Gebeld2' in dbrow and dbrow['Datum Gebeld2'] is not None:
            if ready is False:
                ready = True
                database.mariadb_insert('ticket_status', {
                    'ticket_id': dbrow['Reparatienummer'],
                    'date': str(dbrow['Datum Gebeld2'])[:10] + ' 12:00:00',
                    'volunteer_id': uitgevoerd_door,
                    'status': 'CUSTOMER_INFORMED'})

            database.mariadb_insert('ticket_log', {
                'ticket_id': dbrow['Reparatienummer'],
                'date': str(dbrow['Datum Gebeld2'])[:10] + ' 12:00:00',
                'volunteer_id': uitgevoerd_door,
                'log': dbrow['Reactie Gebeld2']
            })

        if 'Datum Gebeld1' in dbrow and dbrow['Datum Gebeld1'] is not None:
            if ready is False:
                database.mariadb_insert('ticket_status', {
                    'ticket_id': dbrow['Reparatienummer'],
                    'date': str(dbrow['Datum Gebeld1'])[:10] + ' 07:00:00',
                    'volunteer_id': uitgevoerd_door,
                    'status': 'CUSTOMER_INFORMED'})

            database.mariadb_insert('ticket_log', {
                'ticket_id': dbrow['Reparatienummer'],
                'date': str(dbrow['Datum Gebeld1'])[:10] + ' 07:00:00',
                'volunteer_id': uitgevoerd_door,
                'log': dbrow['Reactie Gebeld1']
            })

        # --[ CLOSE ]-------------------------------------------------------------------------------------------------------
        cursor = database.access_acquire_connect()
        cursor.execute(
            'SELECT DISTINCT TOP 1 Tbl_factuur.datum FROM Tbl_factuur RIGHT JOIN (Tbl_Reparaties_main LEFT JOIN Tbl_factuur_omschrijvingen ON Tbl_Reparaties_main.Computernummer = Tbl_factuur_omschrijvingen.Computernummer) ON Tbl_factuur.Factuurnummer = Tbl_factuur_omschrijvingen.factuurnummer WHERE Tbl_factuur.Factuurnummer Is Not Null AND Tbl_Reparaties_main.Reparatienummer={} ORDER BY Tbl_factuur.datum DESC;'.format(
                dbrow['Reparatienummer']))
        invoice_date = cursor.fetchone()

        if invoice_date is not None:
            database.mariadb_insert('ticket_status', {
                'ticket_id': dbrow['Reparatienummer'],
                'date': str(invoice_date[0])[:10] + ' 23:59:59',
                'volunteer_id': uitgevoerd_door,
                'status': 'CLOSED'})

        # --[ LOG ]-------------------------------------------------------------------------------------------------------
        if uitgevoerd_door == 1:
            log = "Migratie heeft medewerker op 1 gezet, mederwerker [" + \
                  str(dbrow['Medewerker']) + \
                  "] heeft de reparatie aangenomen, echter is deze persoon onbekend."
            ticket_log = {
                'ticket_id': dbrow['Reparatienummer'],
                'volunteer_id': 1,
                'log': log
            }
            database.mariadb_insert('ticket_log', ticket_log)


def __import_ticket_logs(ticket_id):
    if ticket_id is not None:
        second = 0
        cursor = database.access_acquire_connect()
        cursor.execute('SELECT * FROM Tbl_Reparaties_uitgediept WHERE Reparatienummer={}'.format(ticket_id))
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            dbrow = dict(zip(columns, row))
            if dbrow['rapport'] is not None:
                database.mariadb_insert('ticket_log', {
                    'ticket_id': ticket_id,
                    'date': str(dbrow['datum'])[:10] + ' 00:00:0' + str(second),
                    'volunteer_id': __lookup_volunteer_id(dbrow['wie']),
                    'log': dbrow['rapport']
                })
                second += 1
