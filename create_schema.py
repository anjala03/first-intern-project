import json
from main import create_schema

def create_schema_bson_generator():
    user_ip = input("enter your file name for json, example schema.json\n >> ")
    if ".json" in user_ip:
        with open(user_ip , "r") as fp:
            schema_refined = json.load(fp)
            create_schema(schema_refined)
        return schema_refined
    else:
        return "no such file"

if __name__ == "__main__":
    print(__name__)