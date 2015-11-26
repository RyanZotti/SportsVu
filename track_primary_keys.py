import pymysql
from pymongo import MongoClient
con = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock',user='root', passwd="", db='NBA')
mysql = con.cursor(pymysql.cursors.DictCursor)
client = MongoClient()
db = client.sportsvu
primary_keys = db.requests.distinct('_id')
for primary_key in primary_keys:
    mysql.execute("""insert into sportsvu_mongodb_primary_keys(game_id, event_id) values("{game_id}","{event_id}")""".format(game_id=primary_key['game_id'],event_id=primary_key['event_id']))
    con.commit()