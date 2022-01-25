import json
import requests

with open('data/dataset.json') as f:
    data = json.load(f)


for earthquake in data:
    url = 'http://localhost:8000/earthquakes/'
    print(earthquake)
    cantonal_losses = earthquake.pop('cantonal_losses')

    response = requests.post(url, json=earthquake)
    response = response.json()
    print(response)
    url = f'{url}{response["id"]}/losses'

    for loss in cantonal_losses:
        rep = requests.post(url, json=loss)
        print(rep)
