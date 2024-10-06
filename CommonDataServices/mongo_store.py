import pymongo
import pandas as pd
import os
# Connect to MongoDB
conn_str=os.getenv("ATLAS_CONN_STR")
#conn_str="mongodb://localhost:27017/"
mongo_client = pymongo.MongoClient(conn_str)
"""db = mongo_client["sample_mflix"]
collection = db["comments"]
# Fetch data from MongoDB and convert to DataFrame
data = list(collection.find({"name":"Mercedes Tyler"}))
df = pd.DataFrame(data)
print(df)"""