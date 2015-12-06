import pymysql
from pymongo import MongoClient
import collections
con = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock',user='root', passwd="", db='NBA')
mysql = con.cursor(pymysql.cursors.DictCursor)
client = MongoClient()
db = client.sportsvu
# Clean up entries from any previous runs
mysql.execute("delete from sportsvu_mongodb_primary_keys")
con.commit()
primary_keys = db.requests.find({},{'_id':1})
# Store all game_id and event_ids
for primary_key in primary_keys:
	game_id = primary_key['_id']['game_id']
	event_id = primary_key['_id']['event_id']
	mysql.execute("""insert into sportsvu_mongodb_primary_keys(game_id, event_id) values("{game_id}","{event_id}")""".format(game_id=game_id,event_id=event_id))
	con.commit()
# Delete incomplete game_id, event_id combinations that aren't recorded as stored in the SportsVuGameIdsStoredInMongo table
mysql.execute('''
	select game_id, event_id from sportsvu_mongodb_primary_keys where not exists (select * from SportsVuGameIdsStoredInMongo where SportsVuGameIdsStoredInMongo.game_id = sportsvu_mongodb_primary_keys.game_id)''')
for row in mysql.fetchall():
	composite_key = collections.OrderedDict()
	composite_key["game_id"]=str(row['game_id'])
	composite_key["event_id"]=int(row['event_id'])
	db.requests.delete_one({'_id': composite_key})
print('Finished')




