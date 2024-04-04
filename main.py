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



# #the migration logic------
# function_return=[]
def mysql_fill_value():
    tables=[]
    def table_names():#gives the table name
        for each_collect in collections:
            tables.append(each_collect)
        return tables
    
    table_list=table_names()# returns the list of tables
    for each_table in table_list:
        print(each_table)
        for tables in collection_docs:
            for dictionaries in tables:
                dict_keys=dictionaries.keys()
                print(dict_keys) #gives the keys-->column name
                values=dictionaries.values() #gives row --> row values
                print(values)
                for ek in dict_keys:
                    if ek.startswith('_id'):
                        primary_key=ek #gives the primary key of each table
                        print(f"pk for {list(dict_keys)} is {primary_key}")
                   

                        


                    
            


         
        
        return {"message":"done till here"}
        
# def the
            

          



    
print(mysql_fill_value())
print(obj.close_connection())



    
    








