
from pymongo_get_database import MongoDB
import os
from dotenv import load_dotenv
from mySql import MySQLclass
import datetime
import json
import difflib


# -------------------------------for mongodb connection-----------------------------------
load_dotenv()
connection_str = os.getenv("Mongo_connection_url")
obj = MongoDB(connection_str)
check = obj.connection_check()
#function call related to mongodb ---
collections = obj.list_collection()
collection_docs = obj.collection_docs()
mongo_table_value = obj.collection_name_and_docs()

#-----------------------------------for sql---------------------------------------------------------
host = os.getenv("host")
port = os.getenv("port")
user = os.getenv("user")
mysql_obj = MySQLclass(host=host, port=port, user=user)
print(f'sql connection {mysql_obj.point_connection()}')


# -------------------------------for mongo data manipulation --------------------------------
def all_table_name():    
    table_name_lists = []#gives the lists of table names
    for each_collection in collections:
        table_name_lists.append(each_collection)
    table = table_fill_values()
    for each_list in table:
        # print(each_list)
        for key,value in each_list[1].items():
            splitted_table_name = []
            if isinstance(value,(list, dict)):
                if key in table_name_lists:
                    key=key + "_"+ "extended_table"
                    #string concatenation 
                if (type(value) == list and (type(value[0]) == dict or type(value[0]) == list)) or (type(value) == dict):
                    splitted_table_name.append(key)
                table_name_lists.extend(splitted_table_name)
    return table_name_lists #returns a list of table names all


def extended_table_values():
    table_values = []
    table = table_fill_values()
    for each_list in table:
        column_names= [] 
        for key,value in each_list[1].items():
            if isinstance(value,(list, dict)):
                if type(value) == list and (type(value[0]) == dict or type(value[0]) == list):
                    column_name = value[0].keys()
                    column_names.append(column_name)
                    row = value[0].values()
                    primary_key = [i for i in column_name if i.startswith("date")]
                    table_values.append((column_name,row, primary_key))

                elif type(value) == dict:
                    column = list(value.values())[0] #the dict value object is first converted to list, as directly the indexing cannot be used on dictvalue object.
                    column_name = column.keys()
                    column_names.append(column_name)
                    row = column.values()
                    primary_key = [i for i in column_name if i.startswith("id")]
                    table_values.append((column_name,row,primary_key))
    return table_values #returns a list with two tuples inside, and each tuple has three attri, first is keys, values and primary key.


def table_fill_values():
    all_table_values = []
    for documents in collection_docs:
        for records in documents:
            table_fields = records.keys()
            table_row = records.values() #gives row --> row values
            for field_name in table_fields:
                if field_name.startswith('_id'):
                    primary_key = field_name #gives the primary key of each table
            all_table_values.append((primary_key, records, table_fields, table_row))      
    return all_table_values #returns a list with three tuple(0,1,2), inside each tuple is a tuple with four attr-pk, alltabledata, colum, rowval


def params_for_sql_query():  
    bson = {}
    table_list = all_table_name()
    extended_table_attributes = extended_table_values()
    for each_table in table_list:
        table_index = table_list.index(each_table)
        bson[each_table] = {}
        table_tuple_set = table_fill_values() 
        for index, table_value in enumerate(table_tuple_set):
            if table_index == index:
                key_val_record  = table_value[1]
                column_name_list = table_value[2]
                for each_column in column_name_list:
                    value = key_val_record.get(each_column)
                    if (each_column.startswith(each_table) or (isinstance(value, (list, dict)))):
                        continue     
                    bson[each_table][each_column] = "None"

        if bson.get(each_table) == {}:
            for each_extended_table in extended_table_attributes:
                column_list = each_extended_table[0]
                table_name=each_table.split("_")[0] #done to resolve the case of tier_and_details table, the probability of match is very low when the whole string is compared with column name
                similar_search=difflib.get_close_matches(table_name, column_list, cutoff=0.6)
                if similar_search:
                    for each_column in column_list:
                        bson[each_table][each_column] = "None"
    return bson

# ----- this function is to create schema, function called on create_schem.py --------
def create_schema(bson):
    for table_name, column_dict in bson.items():
        column_names = column_dict.keys()
        columns = []
        for each_column in column_names:
            columns.append(f"{each_column} {bson.get(table_name).get(each_column)}")
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)});"
        mysql_obj.mycursor.execute(query)
    return True


if __name__ == "__main__":

    # ---------- mysql Close Connecion --------------------

    print(mysql_obj.close_connection())










    




# print(all_table_name())
# print(extended_table_values())
# print(table_fill_values())
# print(obj.close_connection())



    
    







