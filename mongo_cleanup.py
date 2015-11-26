import pymysql
from pymongo import MongoClient
import collections
con = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock',user='root', passwd="", db='NBA')
mysql = con.cursor(pymysql.cursors.DictCursor)
client = MongoClient()
db = client.sportsvu
mysql.execute("select game_id, event_id from sportsvu_cleanup")
for row in mysql.fetchall():
	composite_key = collections.OrderedDict()
	composite_key["game_id"]=str(row['game_id'])
	composite_key["event_id"]=int(row['event_id'])
	db.requests.delete_one({'_id': composite_key})




