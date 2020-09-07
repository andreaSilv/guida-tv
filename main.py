import json
import requests
from datetime import date
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
from multiprocessing.pool import ThreadPool


URL_CANALI = [
    'https://www.staseraintv.com/programmi_stasera_rai1.html',
    'https://www.staseraintv.com/programmi_stasera_rai2.html',
    'https://www.staseraintv.com/programmi_stasera_rai3.html',
    'https://www.staseraintv.com/programmi_stasera_rai4.html',
    'https://www.staseraintv.com/programmi_stasera_rai5.html',
    'https://www.staseraintv.com/programmi_stasera_canale5.html',
    'https://www.staseraintv.com/programmi_stasera_rete4.html',
    'https://www.staseraintv.com/programmi_stasera_italia1.html',
    'https://www.staseraintv.com/programmi_stasera_italia2.html',
    'https://www.staseraintv.com/programmi_stasera_cine34.html',
    'https://www.staseraintv.com/programmi_stasera_iris.html',
    'https://www.staseraintv.com/programmi_stasera_la5.html',
    'https://www.staseraintv.com/programmi_stasera_topcrime.html',
    'https://www.staseraintv.com/programmi_stasera_mediaset_extra.html',
    'https://www.staseraintv.com/programmi_stasera_la7.html',
    'https://www.staseraintv.com/programmi_stasera_la7d.html',
    'https://www.staseraintv.com/programmi_stasera_paramount_channel.html',
    'https://www.staseraintv.com/programmi_stasera_nove.html',
    'https://www.staseraintv.com/programmi_stasera_dmax.html',
    'https://www.staseraintv.com/programmi_stasera_cielo.html',
    'https://www.staseraintv.com/programmi_stasera_tv8.html',
    'https://www.staseraintv.com/programmi_stasera_spike.html'
]

programmi_tv = {}
output_file = None

def main():
    pool = ThreadPool(10)
    results = pool.map(pool_easy_scrap_1, URL_CANALI)
    for i in results:
        programmi_tv.update(i)

    global output_file
    output_file = open("programmi_" + date.today().strftime('%d%m%Y') + ".csv", "w")
    output_file.write('CANALE;ORA;PROGRAMMA;VOTO\n')
    pool.map(pool_easy_scrap_2, programmi_tv.keys())
    output_file.close()


def dowload_page(url):
    response = requests.get(url)
    return response.text


def scrap_page(page):
    global programmi_tv
    soup = BeautifulSoup(page, 'html.parser')
    main_elem = soup.find('div', attrs={ 'class' : 'listingbox'})

    return {
        main_elem.h1.text: [ parse_raw_title(x) for x in main_elem.h4 if x.name != 'br' and x.strip() != '' ]
    }


def parse_raw_title(title):
    title_parts = title.split('-')
    title_parts = [i.strip() for i in title_parts]
    return {
        'ora': title_parts[0].strip(),
        'nome': title_parts[1].strip(),
        'altro': ' - '.join(title_parts[2:]) if len(title_parts) > 2 else None
    }


def title_matching(a, b):
    return SequenceMatcher(None, a, b).ratio()


def search_for_review(name):
    response = requests.get(
        'https://www.mymovies.it/ricerca/ricerca.php',
        params={'limit': 'true', 'q': name}
        )
    accepted_types = ['film']
    
    for i in accepted_types:
        risultati = response.json()['risultati']
        if i in risultati.keys():
            elenco = risultati[i]['elenco']
            for j in elenco:
                if title_matching(j['titolo'].lower(), name.lower()) > 0.8:
                    try:
                        return scrap_review(dowload_page(j['url']))
                    except:
                        return 'Error during scraping'


def scrap_review(page):
    soup = BeautifulSoup(page, 'html.parser')
    elems = soup.findAll('script', attrs={ 'type' : 'application/ld+json'})
    for i in elems:
        parsed_elem = json.loads(i.string)
        if 'aggregateRating' in parsed_elem.keys():
            rating = parsed_elem['aggregateRating']['ratingValue']
            return rating if rating != '' else 0
            break
    return 0


def pool_easy_scrap_1(i):
    return scrap_page(dowload_page(i))


def pool_easy_scrap_2(i):
    for j in programmi_tv[i]:
        voto = search_for_review(j['nome']) if search_for_review(j['nome']) else ''
        csvLine = ";".join([i, j['ora'], j['nome'], voto])
        output_file.write(csvLine + '\n')


if __name__ == "__main__":
    main()