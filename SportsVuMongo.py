import requests
import pandas as pd
import pymysql
import collections
from pymongo import MongoClient
con = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock',user='root', passwd="", db='NBA')
mysql = con.cursor(pymysql.cursors.DictCursor)
client = MongoClient()
db = client.sportsvu # MongoDB database: sportsvu, collection: requests
mysql.execute("select game_id from SportsVuGameIdsUnique where not exists (select * from SportsVuGameIdsStoredInMongo where SportsVuGameIdsStoredInMongo.game_id = SportsVuGameIdsUnique.game_id) limit 1")
for row in mysql.fetchall():
    game_id=row['game_id']
    for event_id in range(1,1000):
        url = "http://stats.nba.com/stats/locations_getmoments/?eventid={event_id}&gameid={game_id}".format(event_id=event_id,game_id=game_id)
        response = requests.get(url)
        if response.status_code == 200:
            composite_key = collections.OrderedDict()
            composite_key['game_id']=game_id
            composite_key['event_id']=event_id
            json = response.json()
            json['_id']=composite_key
            db.requests.insert_one(json)
    mysql.execute("""insert into SportsVuGameIdsStoredInMongo(game_id) values("{game_id}")""".format(game_id=game_id))
    con.commit()
    print(game_id)
con.close()
