import json
from main import create_schema

if __name__ == "__main__":
    user_ip = input("enter your file name for json, example schema.json\n >> ")
    if ".json" in user_ip:
        with open(user_ip , "r") as fp:
            schema_refined = json.load(fp)
            print(create_schema(schema_refined))
    else:
        print("no such file")