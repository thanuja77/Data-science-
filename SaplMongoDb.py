from pymongo import MongoClient   #importing mongoclient from pymongo

#CLIENT CONNECTION
client                   = MongoClient("mongodb://apiUser:nljnuibjbhbsix@78.097.059.59:28917/admin")
                                       # mongodb://  <USERNAME>  :  <PASSWORD>  @   ADDRESS  /  DATABASE
db                       = client['matrix']  # selecting database
txnDetailsCollection     = db["txnDetails"]  # selecting collection



#TO DATAFRAME
import pandas as pd
df                      = pd.DataFrame(list(txnDetailsCollection.find()))  #to dataframe from list of dictionaries
df                      = pd.io.json.json_normalize(list(txnDetailsCollection.find())) #to dataframe from list of dictionaries for nested data 


#QUERY SYNTAX
database.count()
database.find()
database.aggregate()
database.remove()
database.distict()
database.upsert()
database.update()

#UPDATING DATABASE
bulk = collectionname.initializeUnorderedBulkOp()
bulk.find().upsert().update({"$set":{}})
bulk.execute()


#FUNTIONS
"$gte","$gt","$lte","$lt"
"$eq","$ne"
"$in","$nin"
"$exists"


database.aggregate([
    {"$match":{}},
    {"$group":{"_id":{"mobile":"$customerObj.mobile"},"date":{"$last":"$orderDate"}}},
    {"$sort":}
])




