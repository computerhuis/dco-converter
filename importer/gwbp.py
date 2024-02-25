import json
import traceback

from libs import database


def import_postal_codes(filename_in):
    print('Loading json file: ' + filename_in)
    with open(filename_in) as f:
        data = json.load(f)

    for gemeente in data['gemeenten']:
        for wijk in gemeente['wijken']:
            for buurt in wijk['buurten']:
                for postcode in buurt['postcodes']:
                    row = {
                        'code': postcode['postcode'].replace(" ", ""),
                        'street': postcode['straat'],
                        'house_number_min': postcode['nummers'].split("-")[0].strip(),
                        'house_number_max': postcode['nummers'].split("-")[1].strip(),
                        'municipality': gemeente['naam'],
                        'province': gemeente['provincie'],
                        'district': wijk['naam'],
                        'neighbourhood': buurt['naam'],
                        'city': postcode['woonplaats'],
                        'url': postcode['url']
                    }
                    try:
                        if not database.mariadb_exist('postal_codes', 'code', row['code']):
                            database.mariadb_insert('postal_codes', row)
                    except:
                        print("INSERT ERROR: " + json.dumps(row, default=database.json_serial))
                        traceback.print_exc()
    database.mariadb_commit()
