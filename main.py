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
# print("-----LIST OF COLLECTION FROM MONGODB-----")
# print(collections)
# print()

collection_docs=obj.collection_docs()
# print("--------LIST OF COLLECTION_CONTENTS FROM MONGODB-------")
# print(collection_docs)

#for MySql connections---------
obj=MySQL_class(host="127.0.0.1", port="3306",user="anjala_bhatta")
print("------- MYSQL DB CONNECTION CHECK-------")
print(obj.point_connection()) #returns true if connected


table__name_lists=[] #gives the lists of table names
def table_names():
    for each_collection in collections:
        table__name_lists.append(each_collection)
        print(table__name_lists) 
    
    if isinstance([i for i in table_fill_values()[0]], (list, dict)):
        print("hiiiii")
        # print(table_name)
        # table__name_lists.append(table_name)      
    return table__name_lists   

return_values=[]
def table_fill_values():
    for documents in collection_docs:
        for records in documents:
            table_fields=records.keys()
            # print(table_fields) #gives the keys-->column name
            table_row=records.values() #gives row --> row values
            # print(table_records)
            for field_name in table_fields:
                if field_name.startswith('_id'):
                    primary_key=field_name #gives the primary key of each table
                    # print(f"pk for {list(table_fields)} is {primary_key}")
            return_values.append([records, table_fields,table_row, primary_key])             
    return (return_values)

# def for_foreign_key():
#     table_names()

    





    
print(table_names())
# print(table_fill_values())
print(obj.close_connection())



    
    








