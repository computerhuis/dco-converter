import asyncio
import json
import re
import time
import traceback
from datetime import timedelta

from libs import configuration
from libs import soup


async def woonplaats(url):
    print("Haal woonplaats op via url: " + url)
    page = await soup.get_soup(url)
    table = page.find('tbody').findAll('tr')
    return table[6].td.a.text


async def postcode(url, base_url):
    print("Haal postcode op via url: " + url)
    page = await soup.get_soup(url)
    result = []

    table = page.find(id='postcodes-table').find('tbody').findAll('tr')
    for row in table:
        items = row.findAll('td')

        straat = None
        nummers = None
        url = None

        if items[1].a:
            straat = items[1].a.text
        else:
            if items[1].i:
                straat = items[1].i.text
            else:
                straat = items[1].text

        if items[2].a:
            nummers = items[2].a.text
            url = base_url + items[2].a['href']
        else:
            if items[2].i:
                nummers = items[2].i.text
            else:
                nummers = str(items[2].text).strip()

        result.append({
            'postcode': items[0].a.text,
            'straat': straat,
            'nummers': nummers,
            'woonplaats': await woonplaats(url),
            'url': url
        })
    return result


async def buurten(url, base_url):
    print("Haal buurt data op via url: " + url)
    try:
        page = await soup.get_soup(url)
        result = []

        lijst = page.find('h2', string=re.compile(r'^Buurten in de wijk.*$'))
        for item in lijst.parent.findAll('a'):
            buurt_url = base_url + item['href']
            result.append({'naam': item.text,
                           'url': buurt_url,
                           'postcodes': await postcode(buurt_url, base_url)})
    except:
        print("Error on: " + url)

    return result


async def gemeente(grep):
    result = []

    for gemeente in grep['gemeenten']:
        gemeente['wijken'] = []

        print("Haal gemeente [" + gemeente['naam'] + "] data op via url: " + gemeente['url'])
        page = await soup.get_soup(gemeente['url'])

        lijst = page.find('h2', string=re.compile(r'^Wijken van.*$'))
        for item in lijst.parent.findAll('a'):
            wijk_url = grep['base_url'] + item['href']
            gemeente['wijken'].append({'naam': item.text,
                                       'url': wijk_url,
                                       'buurten': await buurten(wijk_url, grep['base_url'])})

        result.append(gemeente)

    return {'gemeenten': result}


async def runner(grep):
    result = await gemeente(grep)

    json_object = json.dumps(result, indent=4, ensure_ascii=False)
    with open("data/postals.json", "w", encoding="utf8") as outfile:
        outfile.write(json_object)


if __name__ == "__main__":
    start_time = time.time()
    print('Access - import data ')
    try:
        configuration.init()
        if configuration.settings['soup']:
            asyncio.run(runner(configuration.settings['soup']))

    except:
        traceback.print_exc()
    print("--- %s duration ---" % timedelta(seconds=round(time.time() - start_time, 2)))
