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
    'season': 14,
}
DOWNLOAD_FOLDER = 'replays/'
DOWNLOAD_AMOUNT = 5000
TOKEN_FILENAME = 'token.txt'
NEXT_URL_FILENAME = 'next.txt'
THREAD = 4

current_file_list = []
count = 0


def pretty(d: dict) -> None:
    """
    Print prettified dictionary.
    """
    print(json.dumps(d, sort_keys=True, indent=2))


def download_replay(replay_id: str, filename: str) -> int:
    """
    Download replay from ballchasing.com.
    returns the amount of retries to download the replay.
    """
    url = 'https://ballchasing.com/dl/replay/' + replay_id
    retries = 0

    while True:
        try:
            response = requests.post(url=url)
        except requests.exceptions.ConnectionError:
            print(f"W: Conection Error downloading {replay_id}")
            continue

        if response.status_code == 200:
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
            raise Exception('ERROR download replay: unknown status code.')

    return retries


def requests_get(*args, **kwargs) -> requests.Response:
    """
    Handled errors on requests.get
    """
    while True:
        try:
            response = requests.get(*args, **kwargs)
        except requests.exceptions.ConnectionError:
            print(f"W: Conection Error on {args[0]}")
            continue

        if response.status_code == 429:
            sleep(1)
        else:
            break

    return response


def main() -> None:
    """
    Main program
    """
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
            thread_list.append(
                DownloadReplayThread(f't{n}', data['list'], thread_lock))

        for t in thread_list:
            t.join()

        print('fetch next replays...')
        if 'next' in data:
            next_url = data['next']

            # save next url
            with open(NEXT_URL_FILENAME, 'w') as next_file:
                next_file.write(next_url + '\n')

            response = requests_get(next_url, headers=API_HEADERS)
            print(response.url)
        else:
            print('No next url.')
            with open(NEXT_URL_FILENAME, 'w') as next_file:
                next_file.write('\n')
            break


class DownloadReplayThread(threading.Thread):
    """
    Thread for downloading replays.
    get the next replay from a shared data list.
    """
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
            try:
                self.lock.acquire()
                d = self.data.pop()
            except IndexError:
                break
            finally:
                self.lock.release()

            filename = d['id'] + '.replay'
            if filename not in current_file_list:
                retries = download_replay(d['id'], DOWNLOAD_FOLDER + filename)

                self.lock.acquire()
                count += 1
                current_file_list.append(filename)
                print(f"{self.thread_id}: {count} - downloaded {d['id']}",
                      f"({retries}) {d['map_name']}")
                enough = count >= DOWNLOAD_AMOUNT
                self.lock.release()

            if enough:
                break

        print(f"{self.thread_id}: stopped")


if __name__ == '__main__':
    # setup
    with open(TOKEN_FILENAME, 'r') as token_file:
        token = token_file.readline().strip()
        API_HEADERS.update({'Authorization': token})

    current_file_list = listdir(DOWNLOAD_FOLDER)
    count = len(current_file_list)

    main()
    print('done')
