import json
from main import params_for_sql_query

if __name__ == "__main__":

    # --------- Get Schema -------------
    user_ip = input("enter your file name for json, example schema_none.json\n >> ")
    sql_schema_json = params_for_sql_query()
    user_ip = user_ip if '.json' in user_ip else "schema_none.json"
    with open(user_ip , "w") as fp:
        json.dump(sql_schema_json , fp)