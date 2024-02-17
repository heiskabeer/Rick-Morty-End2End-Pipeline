import requests
from requests.exceptions import RequestException
import json
import time
import pandas as pd
# import logging
from pathlib import Path
from prefect import task, flow, get_run_logger

# logging.basicConfig(level=logging.INFO)
# Replace python logging with PREFECTS


@task
def get_pages():
    try:
        logging = get_run_logger()
        url = f'https://rickandmortyapi.com/api/character/'
        response = requests.get(url)
        data = response.json()
        pages = data['info']['pages']
        logging.info(f'Pages: {pages}')
    except RequestException as e:
        logging.error(f'RequestException: {e}')
    except Exception as e:
        logging.error(f'Exception: {e}', exc_info=True)
    return pages


@task
def get_all_characters_data(pages):
    try:
        logging = get_run_logger()
        for x in range(1, (pages) + 1):
            url = 'https://rickandmortyapi.com/api/character/?page='
            response = requests.get(url + str(x))
            data = response.json()
            yield data 

            ''' using yield lazy evaluation feature to proecess the response per page,
            and avoid loading all the api response into memory '''

    except RequestException as e:
        logging.error(f'RequestException: {e}')
    except Exception as e:
        logging.error(f'Exception: {e}', exc_info=True)

            
@task
def parse_data(all_characters_data):

    logging = get_run_logger()

    characters_list = []

    for page_data in all_characters_data:

        for character in page_data['results']:
                
                characters = {

                'id': character['id'],

                'name' : character['name'],

                'image' : character['image'],

                'specie' : character['species'],

                'gender' : character['gender'],

                'origin' : character['origin']['name'],

                'status' : character['status'],

                'no_of_episodes' : len(character['episode'])
                    
                }

                characters_list.append(characters)
    
    logging.info(f'Characters Found: {len(characters_list)}')

    return characters_list


@task
def convert_to_csv(character_lists):
    df = pd.DataFrame(character_lists)
    # output_path = Path(__file__).parent.parent.resolve().joinpath('data', 'output', 'rick.csv')
    output_path = Path(r'C:\Users\hp\Documents\Data\DE-Projects\rick-s3-lamda-snowflake\data\output').joinpath('ricky.csv')
    df.to_csv(output_path, index=False,)


@flow
def main():
    logging = get_run_logger()
    start_time = time.perf_counter()

    pages = get_pages()

    character_data = get_all_characters_data(pages)
    parsed = parse_data(character_data)
    convert_to_csv(parsed)

    end_time = time.perf_counter()
    t3 = end_time - start_time 
    logging.info(f'Elapsed Time: {t3}')


if __name__ == '__main__':
    main()
    
