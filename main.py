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
print(check)
#function call related to mongodb ---
collections=obj.list_collection()
# print("-----LIST OF COLLECTION FROM MONGODB-----")
# print(collections)
# print()

collection_docs=obj.collection_docs()
# print("--------LIST OF COLLECTION_CONTENTS FROM MONGODB-------")
# print(collection_docs)

#for MySql connections---------
# obj=MySQL_class(host="127.0.0.1", port="3306",user="anjala_bhatta")
# print("------- MYSQL DB CONNECTION CHECK-------")
# print(obj.point_connection()) #returns true if connected


def all_table_name():    
    table_name_lists=[]#gives the lists of table names
    for each_collection in collections:
        table_name_lists.append(each_collection)
    table = table_fill_values()
    for each_list in table:
        for key,value in each_list[1].items():
            splitted_table_name=[]
            if isinstance(value,(list, dict)):
                if type(value)==list and (type(value[0])==dict or type(value[0])==list):
                    splitted_table_name.append(key)

                elif type(value)==dict:
                    splitted_table_name.append(key)
                table_name_lists.extend(splitted_table_name)
    return (table_name_lists) #returns a list of table names all


def extended_table_values():
    table_values=[]
    table = table_fill_values()
    for each_list in table:
        column_names=[] 
        for key,value in each_list[1].items():
            if isinstance(value,(list, dict)):
                if type(value)==list and (type(value[0])==dict or type(value[0])==list):
                    column_name=value[0].keys()
                    column_names.append(column_name)
                    row=value[0].values()
                    primarykey= [i for i in column_name if i.startswith("date")]
                    table_values.append((column_name,row, primarykey))

                elif type(value)==dict:
                    column=list(value.values())[0] #the dict value object is first converted to list, as directly the indexing cannot be used on dictvalue object.
                    column_name=column.keys()
                    column_names.append(column_name)
                    row=column.values()
                    primarykey= [i for i in column_name if i.startswith("id")]
                    table_values.append((column_name,row,primarykey))
    return (table_values) #returns a list with two tuples inside, and each tuple has three attri, first is keys, values and primary key.



def table_fill_values():
    all_table_values=[]
    for documents in collection_docs:
        for records in documents:
            table_fields=records.keys()
            table_row=records.values() #gives row --> row values
            for field_name in table_fields:
                if field_name.startswith('_id'):
                    primary_key=field_name #gives the primary key of each table
            all_table_values.append((primary_key, records, table_fields, table_row))      
    return all_table_values #returns a list with three tuple(0,1,2), inside each tuple is a tuple with four attr-pk, alltabledata, colum, rowval

    
#-----------------------------------for sql---------------------------------------------------------
obj=MySQL_class(host="127.0.0.1", port="3306",user="anjala_bhatta")
print(obj.point_connection())


print("hiiiiii")
print(obj.mycursor.execute("SHOW DATABASES;"))
print("hereeeee")
obj.mycursor.execute("CREATE TABLE anjala(id INT, name VARCHAR[100]);")
print('hellloooooo')
print(obj.close_connection())









    




# print(all_table_name())
# print(extended_table_values())
# print(table_fill_values())
# print(obj.close_connection())



    
    








