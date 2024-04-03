from pymongo import MongoClient
from pymongo_get_database import Mongo_DB
import os
from dotenv import load_dotenv
from mySql import MySQL_class


#for mongodb connection--------
load_dotenv()
connection_str=os.getenv("Mongo_connection_url")
obj=Mongo_DB(connection_str)
check=obj.connection_check()

#function call related to mongodb ---
collections=obj.list_collection()
print(collections)
print()

collection_docs=obj.collection_docs()
print(collection_docs)#prints out all the mongo collection contents

#for MySql connections---------
obj=MySQL_class(host="127.0.0.1", port="3306",user="anjala_bhatta")
print(obj.point_connection()) #returns true if connected


# #the migration logic------
def mysql_schema_creation():
    for tables in collection_docs:
        for attributes in tables:
            print(attributes)
            print()
            dict_keys=attributes.keys()
            print(dict_keys)
            keys_list=(list(dict_keys)) 
            # for each in keys_list:
               

            # keys_list=(list(dict_keys)) 
            # print()
            # print(keys_list)
    return {"message":"done till here"}


    # for each_table in collections:
    #     query_str=f"CREATE TABLE {each_table}(f)"
    #     obj.mycursor.execute(query_str)
    #     obj.my_db.commit()
    # return {"message":"tables created"}
print(mysql_schema_creation())
print(obj.close_connection())



    
    








