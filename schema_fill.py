import json 
from main import mongo_table_value
from create_schema import create_schema_bson_generator

#-------function that handles the foreign key relation in schema.json--------------
schema_refined = create_schema_bson_generator()
def handle_foreign_key(column_name, schema_refined, table_name):
        if column_name == "FOREIGN KEY":
            column_type = schema_refined.get(table_name).get(column_name)
            multiple_foreign_key = column_type.split(",")
            foreign_key_list = [multiple_foreign_key[0]]
            each_foreign = [i.replace("FOREIGN KEY ", "") for i in multiple_foreign_key[1:]]
            foreign_key_list.extend(each_foreign)
            empty_list = []
            for each in foreign_key_list:
                two_parts = each.replace(" ON DELETE CASCADE", "").split(" REFERENCES ")
                first_part = two_parts[0].replace("(", "").replace(")", "")
                print('first',two_parts)
                second_part_table_name = (two_parts[1].split("(")[0])
                print('secomd',empty_list)
                second_part_column_name = (two_parts[1].split("(")[1]).replace(")", "") 
                empty_list.append([first_part,[second_part_table_name, second_part_column_name]])
            return (True, empty_list)
        return (False, "Not a foreign key")
            

        
def insert_value_in_schema(mongo_table_value, formed_schema):
    # --- FROM formed schmea -------------
    for table_name, values in formed_schema.items():
        all_column_in_schema = list(values.keys())
        for each_column in all_column_in_schema[::-1]:
        # -- Mongo table values ------
            documents = mongo_table_value.get(table_name)
            try:
                for each_document in documents:
                    table_dict = {}
                    all_keys = each_document.keys()
                    for each_key in all_keys:
                        # TO GET THE COLUMN VALUE columnn_value = each_document.get(each_key)
                        # Handling foreign key condition
                        foreign_key = handle_foreign_key(each_column, formed_schema, table_name)
                        is_foreign = foreign_key[0]
                        if is_foreign:
                            try:
                                for each_column_table_list in foreign_key[1]:
                                    if each_key == each_column_table_list[0]:
                                        foreign_table_name_and_pk = each_column_table_list[1]
                                        foreign_table_name = foreign_table_name_and_pk[0]
                                        foreign_table_primary_key = foreign_table_name_and_pk[1]
                                        # Only fill the foreign table if the foreign key dont have a table 
                                        if not foreign_table_name in (key for key in mongo_table_value.keys()):
                                            fill_foreign_table = {}
                                            foreign_columns = formed_schema.get(foreign_table_name)
                                            for each_foreign_column in foreign_columns:
                                                # Exception case for tier and details as there is no auto increment for it
                                                print(f'here foreign table {foreign_table_name}')
                                                if each_foreign_column == "id" and foreign_table_name != "tier_and_details" :
                                                      # Continue is there to simply skip the auto increment primary key case
                                                     continue
                                                else:
                                                    foreign_table_values = each_document.get(each_key)
                                                    for each_value in foreign_table_values:
                                                        if type(each_value) is dict:
                                                              for each_key in each_value.keys():
                                                                  print("hiiiiiieach_value", each_value)
                                                                  if each_key == each_foreign_column:
                                                                    print(f' errrorrrrrr {each_value[each_foreign_column]}')
                                                                    fill_foreign_table[each_foreign_column] = each_value[each_foreign_column]
                                                        
                                                        else:
                                                            print(f'else block {fill_foreign_table[each_foreign_column]}: {each_value} ')
                                                            fill_foreign_table[each_foreign_column] = each_value
                                            print("fill_foreign_table", fill_foreign_table)
                                                                

                                        else:
                                            pass
                                                            
                            except Exception as e:
                                print(e)
                        elif each_column == "_id":
                            print("is a primary key", each_column)
                            continue
                        else:
                            print('each-column', each_column)
                            column_value = each_document.get(each_key)
                            if not isinstance(column_value, type(str)):
                                column_value = str(column_value)
                            table_dict[each_column] = column_value
                    query = f"INSERT INTO {table_name} ({",".join(key for key in table_dict.keys())}) VALUES({",".join(value for value in table_dict.values())});" 
                    # print(query)
            except Exception as e:
                    print(e)
                                 

if __name__ == "__main__":
    # ---------populate schema ------------------
    insert_value_in_schema(mongo_table_value, schema_refined)