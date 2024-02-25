import asyncio
import json
import os
import sys
from datetime import datetime

CONFIGURATION_FILE = 'settings.json'
global settings
global running_timestamp


# --[ LOAD ]------------------------------------------------------------------------------------------------------------
def init():
    global settings, running_timestamp
    settings = None
    running_timestamp = datetime.now()

    if not os.path.isfile(CONFIGURATION_FILE):
        print('No configuration file found, an example  configuration file was created: ', CONFIGURATION_FILE)
        with open(CONFIGURATION_FILE, 'w') as outfile:
            example = {
                "import_date_from": "2020-01-01",
                "databases": {
                "pgsql": {
                    "connection": None,
                    "pool": None
                },
                "access": {
                    "connection": None,
                    "pool": None
                }
            },
                "load": {
                    "gemeenten": []
                },
                "soup": {
                    "url": None
                },
                "debug": {
                    "sql": False,
                    "soup": False,
                    "curl": False
                },
            }
            json.dump(example, outfile, sort_keys=True, indent=4)
    else:
        with open(CONFIGURATION_FILE) as json_file:
            settings = json.load(json_file)
            if settings:
                if sys.platform == 'win32':
                    # https://stackoverflow.com/questions/58422817/jupyter-notebook-with-python-3-8-notimplementederror
                    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
