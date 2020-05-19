from os import listdir
from sys import stdout
from time import sleep
import json
import math
import threading

import requests

API_HEADERS = {
    'Authorization': None,
}
GET_REPLAYS_PARAMS = {
    'min-rank': 'grand-champion',
    'max-rank': 'grand-champion',
    'playlist': 'ranked-duels',
    'map': 'stadium_p',
}
DOWNLOAD_FOLDER = 'replays/'
DOWNLOAD_AMOUNT = 1000
TOKEN_FILENAME = 'token.txt'
NEXT_URL_FILENAME = 'next.txt'
THREAD = 4

current_file_list = []
count = 0

def pretty(d: dict) -> None:
    print(json.dumps(d, sort_keys=True, indent=2))

def download_replay(replay_id: str, filename: str) -> int:
    url = 'https://ballchasing.com/dl/replay/' + replay_id
    retries = 0

    while True:
        try:
            response = requests.post(url=url)
        except requests.exceptions.ConnectionError:
            print(f"Conection Error {replay_id}")
            continue

        if response.status_code == 200:
            # Write the file contents in the response to a file specified by local_file_path
            with open(filename, 'wb') as local_file:
                for chunk in response.iter_content(chunk_size=1024):
                    local_file.write(chunk)
            break

        elif response.status_code == 429:
            retries += 1
            sleep(1)

        else:
            print('error', response)
            print(response.text)
            raise Exception('ERROR download replay: STATUS_CODE')

    return retries

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

        thread_list = []
        thread_lock = threading.Lock()
        for n in range(THREAD):
            thread_list.append(DownloadBatch(f't{n}', data['list'], thread_lock))

        for t in thread_list:
            t.join()

        print('fetch new data...')
        next_url = data['next']

        # save next url
        with open(NEXT_URL_FILENAME, 'w') as next_file:
            next_file.write(next_url + '\n')

        response = requests_get(next_url, headers=API_HEADERS)

class DownloadBatch(threading.Thread):

    def __init__(self, thread_id: str, data: list, lock: threading.Lock):
        super().__init__()
        self.thread_id = thread_id
        self.data = data
        self.lock = lock
        self.start()

    def run(self):
        global count, current_file_list

        enough = False
        while True:
            self.lock.acquire()
            try:
                d = self.data.pop()
            except IndexError:
                print(f"ending {self.thread_id}")
                self.lock.release()
                return
            self.lock.release()

            filename = d['id'] + '.replay'
            if filename not in current_file_list:
                retries = download_replay(d['id'], DOWNLOAD_FOLDER + filename)

                self.lock.acquire()
                count += 1
                current_file_list.append(filename)
                print(f"{self.thread_id}: {count} - downloaded {d['id']} ({retries})")
                enough = count >= DOWNLOAD_AMOUNT
                self.lock.release()

            if enough:
                print(f"ending {self.thread_id}")
                return


if __name__ == '__main__':
    # setup
    with open(TOKEN_FILENAME, 'r') as token_file:
        token = token_file.readline().strip()
        API_HEADERS.update({'Authorization': token})

    current_file_list = listdir(DOWNLOAD_FOLDER)
    count = len(current_file_list)

    main()
    print('done')
