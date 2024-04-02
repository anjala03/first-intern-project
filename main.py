from pymongo import MongoClient
from .pymongo_get_database import Mongo_DB
from .mySql import MySQL_class


connection_str="mongodb+srv://py-trainee:jMaDl9Lyn03r2zT3@trainee-cluster.ojubn5t.mongodb.net/?retryWrites=true&w=majority&appName=trainee-cluster"
obj=Mongo_DB(connection_str)
obj.connection_check()

# my_db=client.sample_analytics
#         collection1=my_db.accounts
#         # collec2=my_db.customers
#         # collec3=my_db.transactions
#         cursor=collection1.find()
#         print(cursor)
#         for each_col in cursor:
#             print(each_col["products"])

