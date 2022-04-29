from pymongo import MongoClient
client = MongoClient()
client = MongoClient('mongodb://localhost:27017/')
db = client.dbt
d = db["tweets"];

mydict = { "IPL": 1}
mycollection.update_one({'_id':mongo_id}, {"$set": post}, upsert=False)
d.insert_one(mydict);