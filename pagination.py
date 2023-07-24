import requests
import time
import pandas as pd

url = "https://rickandmortyapi.com/api/character?page="

page = 1
while True:
    response = requests.get(url + str(page))
    data = response.json()

    if 'error' in data:
        break

    print(data)  # or do something with the data

    page += 1
    time.sleep(1)  # optional delay to avoid overloading the server

df = pd.DataFrame(data)