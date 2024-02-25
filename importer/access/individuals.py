import traceback
from json import dumps

from importer.access import cleanup_house_number, cleanup_huisnummertoevoeging, cleanup_postal_code, postal_code_exist
from libs import configuration
from libs import database


def export_access_import_mariadb():
    if configuration.settings['databases']['access']:
        cursor = database.access_acquire_connect()
        cursor.execute('SELECT Tbl_Gebruikers_NAW.* FROM Tbl_Gebruikers_NAW WHERE [Datum Inschrijving] > #' +
                       configuration.settings[
                           'import_date_from'] + '#')
        columns = [column[0] for column in cursor.description]
        for row in cursor.fetchall():
            __import_individual(dict(zip(columns, row)))


def import_individual(id):
    print("Lookup individual with id: " + str(id))
    if id is not None:
        if configuration.settings['databases']['access']:
            cursor = database.access_acquire_connect()
            cursor.execute('SELECT Tbl_Gebruikers_NAW.* FROM Tbl_Gebruikers_NAW WHERE Gebruikersnummer=' + str(id))
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                __import_individual(dict(zip(columns, row)))


def __import_individual(dbrow):
    if dbrow is not None:
        if not database.mariadb_exist('individuals', 'id', dbrow['Gebruikersnummer']):
            print("Import individual with id: " + str(dbrow['Gebruikersnummer']))
            huisnummer = cleanup_house_number(dbrow['Huisnummer'])
            huisnummertoevoeging = cleanup_huisnummertoevoeging(huisnummer, dbrow['Huisnummer'])

            mobile = None
            telefoon = None
            if dbrow['1e Telefoon'] and str(dbrow['1e Telefoon'])[:2] == '06':
                mobile = dbrow['1e Telefoon']
            else:
                telefoon = dbrow['1e Telefoon']

            if dbrow['2e Telefoon'] and str(dbrow['2e Telefoon'])[:2] == '06':
                mobile = dbrow['2e Telefoon']
            else:
                telefoon = dbrow['2e Telefoon']

            woonplaats = "'s-Hertogenbosch"
            if dbrow['Plaatsnaam']:
                woonplaats = dbrow['Plaatsnaam']

            persoon = {'id': dbrow['Gebruikersnummer'],
                       'initials': dbrow['Voorletters'],
                       'first_name': dbrow['Voornaam'],
                       'infix': dbrow['Tussenvoegsels'],
                       'last_name': dbrow['Achternaam'],
                       'date_of_birth': dbrow['Geboortedatum'],
                       'email': dbrow['E-Mailadres'],
                       'mobile': mobile,
                       'telephone': telefoon,
                       'postal_code': cleanup_postal_code(dbrow['Postcode']),
                       'street': dbrow['Adres'],
                       'house_number': huisnummer,
                       'house_number_addition': huisnummertoevoeging,
                       'city': woonplaats,
                       'registered': dbrow['Datum Inschrijving'],
                       'comments': dbrow['Opmerkingen'],
                       'msaccess': dumps(dbrow, default=database.json_serial)}

            if not persoon['first_name']:
                persoon['first_name'] = '_onbekend_'

            if not persoon['last_name']:
                persoon['last_name'] = '_onbekend_'

            if not postal_code_exist(persoon['postal_code'], persoon['house_number']):
                del (persoon['postal_code'])
                del (persoon['house_number'])
                del (persoon['house_number_addition'])

            try:
                database.mariadb_insert('individuals', persoon)
                database.mariadb_commit()

                # Check if person is also a user
                if (dbrow['Bedrijfsnummer']) == 6 or (dbrow['Bedrijfsnummer'] == 1):
                    user_type = 'VOLUNTEER'
                    if dbrow['Bedrijfsnummer'] == 6:
                        user_type = 'CANDIDATE'

                    login = {
                        'volunteer_id': dbrow['Gebruikersnummer'],
                        'username': dbrow['E-Mailadres'],
                        'user_type': user_type
                    }
                    database.mariadb_insert('individual_login', login)
                    database.mariadb_commit()

                    for role in ['COUNTER', 'EDUCATION', 'WORKSHOP']:
                        database.mariadb_insert('individual_authorities', {
                            'username': dbrow['E-Mailadres'],
                            'authority': role
                        })
                        database.mariadb_commit()
            except:
                print("INSERT ERROR: " + dumps(persoon, default=database.json_serial))
                traceback.print_exc()
                exit(1)
