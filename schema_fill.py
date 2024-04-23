import json 
from main import mongo_table_value
from create_schema import create_schema_bson_generator

schema_refined = create_schema_bson_generator()

#-------function that handles the foreign key relation in schema.json--------------
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

class PopulateSchema(mongo_table_value, schema_refined):
    
    def __init__(self, mongo_table_value, schema_refined):
        self.mongo_table_value = mongo_table_value
        self.formed_schema = schema_refined


    def getting_value_from_mongo(self):
        for table_name, records in self.mongo_table_value.items():
            documents = self.mongo_table_value.get(table_name)
            print(documents)
            try:
                for each_document in documents:
                    all_keys = each_document.keys()
                    for each_key in all_keys:
                        pass
            except Exception as err:
                print(err)

        

    def insert_value_in_schema(self):
            documents = self.mongo_table_value.get(table_name)
            try:
                for each_document in documents:
                    all_keys = each_document.keys()
                    for each_key in all_keys:
                        pass
            except Exception as err:
                print(err)

            for table_name, values in self.formed_schema.items():
                table_dict = {}
                all_column = list(values.keys())
                for each_column in all_column[::-1]:
                    if each_column == "FOREIGN KEY":
                        foreign_key = handle_foreign_key(each_column, self.formed_schema, table_name)
                        print(f'this is the foreign keys {foreign_key}')
                        status = foreign_key[0]
                    elif each_column == "_id":
                        print("primary key")
                        continue
                    else:
                        column_value = each_document.get(each_key)
                        print("column_value=", column_value)
                        if not isinstance(column_value, type(str)):
                            column_value = str(column_value)
                        table_dict[each_column] = column_value
            query = f"INSERT INTO {table_name} ({",".join(key for key in table_dict.keys())}) VALUES({",".join(value for value in table_dict.values())});" 
            print(query)
                # --------------   getting the table_value from mongo --------
                    
                #     try:
                #         for each_document in documents:
                #             all_keys = each_document.keys()
                #             for each_key in all_keys:
                #                 print(f'each_key = {each_key}, and each_column = {each_column}')
                #                 if each_key == "_id":
                #                     print("primary key")
                #                     continue
                #                 elif each_key == each_column:
                #                     for each_column_table_list in foreign_key[1]:
                #                         if each_column in each_column_table_list[0]:
                #                             print('its a foreign key or column ')
                #                             extend_table_name_and_column = each_column_table_list[1]
                #                             ext_table_name = extend_table_name_and_column[0]
                #                             ext_column_name = extend_table_name_and_column[1]
                #                             foreign_table_values = each_document.get(each_key)
                #                         # if isinstance(foreign_table_values, type(list)):
                #                         #     print("foreignkeyyyyyy")
                #                         #     for each_record in foreign_table_values:
                #                         #         try:
                #                         #             if type(each_record) is dict:
                #                         #                 pass
                #                         #         except Exception as e:
                #                         #             print(e)
                #                         #         else:
                #                         #             print('foreignhandle')
                #                         #             # query = f"INSERT INTO {ext_table_name} (products) VALUES(i for i in {foreign_table_values})"
                #                         #             # print(f'query for foreign table {query}')
                #                         # elif isinstance(foreign_table_values, type(dict)):
                #                         #     pass
                #                     else:
                #                         # THIS IS NOT THE FOREIGN KEY CONDITION ONLY 
                #                         column_value = each_document.get(each_key)
                #                         print("column_value=", column_value)
                #                         if not isinstance(column_value, type(str)):
                #                             column_value = str(column_value)
                #                         table_dict[each_column] = column_value
                #                 else:
                #                     print("column name didnot match ")
                #     except Exception as err:
                #         print(err)
                # query = f"INSERT INTO {table_name} ({",".join(key for key in table_dict.keys())}) VALUES({",".join(value for value in table_dict.values())});" 
                # print(query)

if __name__ == "__main__":

    # ---------populate schema ------------------
    class_obj = PopulateSchema(mongo_table_value, schema_refined)
            
        
    print(class_obj.insert_value_in_schema())