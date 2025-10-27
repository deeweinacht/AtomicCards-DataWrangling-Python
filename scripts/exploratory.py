import json

raw = json.load(open("data/raw/atomiccards.json", "r", encoding="utf-8"))
data = raw['data']

first_key = next(iter(data))

for dp in data[first_key]:
    print(dp)
