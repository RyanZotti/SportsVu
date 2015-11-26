import pymysql
from pymongo import MongoClient
con = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock',user='root', passwd="", db='NBA')
mysql = con.cursor(pymysql.cursors.DictCursor)
client = MongoClient()
db = client.sportsvu
primary_keys = db.requests.find({},{'_id':1})
for primary_key in primary_keys:
	game_id = primary_key['_id']['game_id']
	event_id = primary_key['_id']['event_id']
	mysql.execute("""insert into sportsvu_mongodb_primary_keys(game_id, event_id) values("{game_id}","{event_id}")""".format(game_id=game_id,event_id=event_id))
	con.commit()