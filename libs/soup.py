import aiohttp
from bs4 import BeautifulSoup

from libs import configuration


async def load_url(url):
    if configuration.settings['debug']['soup']:
        print("Loading url: " + url)

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.read()


async def load_only_200_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return await resp.read()
            else:
                return False


async def get_soup(url):
    text = await load_url(url)
    return BeautifulSoup(text.decode('utf-8'), 'html5lib')


async def get_only_200_soup(url):
    text = await load_only_200_url(url)
    if text:
        return BeautifulSoup(text.decode('utf-8'), 'html5lib')
    else:
        return False
