from os import listdir
from sys import stdout
from time import sleep

import json
import requests

API_HEADERS = {
    'Authorization': None,
}
GET_REPLAYS_PARAMS = {
    'min-rank': 'grand-champion',
    'max-rank': 'grand-champion',
    'playlist': 'ranked-duels',
    'map': 'eurostadium_p',
}
DOWNLOAD_FOLDER = 'replays/'
DOWNLOAD_AMOUNT = 1000
TOKEN_FILENAME = 'token.txt'
NEXT_URL_FILENAME = 'next.txt'

current_file_list = []

def pretty(d: dict) -> None:
    print(json.dumps(d, sort_keys=True, indent=2))

def download_replay(replay_id: str, filename: str) -> None:
    url = 'https://ballchasing.com/dl/replay/' + replay_id

    stdout.write(f"{len(current_file_list) + 1}: downloading {replay_id}")
    stdout.flush()

    response = requests.post(url=url)
    while True:
        if response.status_code == 200:
            # Write the file contents in the response to a file specified by local_file_path
            with open(filename, 'wb') as local_file:
                for chunk in response.iter_content(chunk_size=1024):
                    local_file.write(chunk)
            break

        elif response.status_code == 429:
            stdout.write(".")
            stdout.flush()
            sleep(1)
            response = requests.post(url=url)

        else:
            print('error', response)
            print(response.text)
            raise Exception('ERROR download replay: STATUS_CODE')

    stdout.write(" V\n")
    stdout.flush()

def requests_get(*args, **kwargs) -> requests.Response:
    response = requests.get(*args, **kwargs)

    while True:
        if response.status_code == 429:
            sleep(1)
            response = requests.get(*args, **kwargs)
        else:
            break

    return response

def main() -> None:
    # prepare
    count = len(current_file_list)

    with open(NEXT_URL_FILENAME, 'r') as next_file:
        next_url = next_file.readline().strip()

    if next_url == '':
        response = requests_get('https://ballchasing.com/api/replays',
                            params=GET_REPLAYS_PARAMS,
                            headers=API_HEADERS)
    else:
        response = requests_get(next_url, headers=API_HEADERS)

    # download until the limit
    while count < DOWNLOAD_AMOUNT:
        data = response.json()

        for d in data['list']:
            filename = d['id'] + '.replay'
            if filename not in current_file_list:
                download_replay(d['id'], DOWNLOAD_FOLDER + filename)
                count += 1
                current_file_list.append(filename)

            if count >= DOWNLOAD_AMOUNT:
                break

        print('fetch new data...')
        next_url = data['next']

        # save next url
        with open(NEXT_URL_FILENAME, 'w') as next_file:
            next_file.write(next_url + '\n')

        response = requests_get(next_url, headers=API_HEADERS)


if __name__ == '__main__':
    # setup
    with open(TOKEN_FILENAME, 'r') as token_file:
        token = token_file.readline().strip()
        API_HEADERS.update({'Authorization': token})
    current_file_list = listdir(DOWNLOAD_FOLDER)

    main()
    print('done')
