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
# print("------- MYSQL DB CONNECTION CHECK-------")
obj.point_connection() #returns true if connected


def table_names():    
    table__name_lists=[]#gives the lists of table names
    for each_collection in collections:
        table__name_lists.append(each_collection)
    table = table_fill_values()
    for each_list in table:
        for key,value in each_list[1].items():
            if isinstance(value,(list, dict)):   
                splitted_table_name=[]
                if type(value)==list and type(value[0])==dict:
                    splitted_table_name.append(key)
                    table_field=value[0].keys()
                    table_values=value[0].values()
                    print(table_field)
                if type(value)==dict:
                    table_field=[for key, value in value.values()]
                    print(table_field)


                    splitted_table_name.append(key)
                table__name_lists.extend(splitted_table_name)
    return table__name_lists   


def table_fill_values():
    all_table_values=[]
    for documents in collection_docs:
        for records in documents:
            table_fields=records.keys()
            table_row=records.values() #gives row --> row values
            for field_name in table_fields:
                if field_name.startswith('_id'):
                    primary_key=field_name #gives the primary key of each table
            all_table_values.append([primary_key, records, table_fields, table_row])      
    return all_table_values


# def for_splitted_table():
    
#     # tables=[i for i in table_names()]
#     # if tables[3:]:
        
#     #     fields=
#     #     rows=
    







    




print(table_names())
# table_fill_values()
# print(for_splitted_table())
print(obj.close_connection())



    
    








