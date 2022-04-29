from pymongo import MongoClient
client = MongoClient()
client = MongoClient('mongodb://localhost:27017/')
db = client.dbt
d = db["tweets"];

mydict = { "name": "John", "address": "Highway 37" }
d.insert_one(mydict);