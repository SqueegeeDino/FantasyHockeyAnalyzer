import requests as rq
import json

apiConfig = "https://api.nhle.com/stats/rest/en/config"
apiTest = "https://api.nhle.com/stats/rest/en/skater/realtime?isAggregate=true&isGame=false&limit=-1&cayenneExp=seasonId=20232024 and gameTypeId=2 and playerId=8480800"

def dumpAPI(dumpee, filename:str):
    output_filename = filename
    response = rq.get(dumpee)
    if response.status_code == 200:
        data = response.json()
        with open(output_filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Dumping response to: {filename}.json")
    else:
        print(f"API return error:{response.status_code}")

dumpAPI(apiTest, "dumpTest")