from pymongo import MongoClient
from pymongo_get_database import Mongo_DB
from mySql import MySQL_class
import os
from dotenv import load_dotenv
import mysql.connector

#for mongodb connection--------
load_dotenv()
connection_str=os.getenv("Mongo_connection_url")
obj=Mongo_DB(connection_str)
check=obj.connection_check()
collections=obj.list_collection()
print(collections)#prints out all the mongo collection contents


#for MySql connections---------
obj=MySQL_class(host="127.0.0.1", port="3306",user="anjala",password="anjala@xampp")
print(obj.point_connection()) #returns true if connected
obj.make_schema()

#the migration logic------
def migrate_dataset():
    #here refine the data and insert into mysql db logicc
    source_database=collections
    # destination_database=




    return


migrate=migrate_dataset()






