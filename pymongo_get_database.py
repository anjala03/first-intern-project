from pymongo import MongoClient

class Mongo_DB:

    def __init__(self, connection_str):
        self.connection=connection_str #connection formed  
    
    def connection_check(self):
        self.client=MongoClient(self.connection)
        if self.client:
            return {"message":"Connected to mongo_db"}
        return {"message":"Not Connected to mongo_db"}
    
    def list_collection(self):
        if self.client!=None:
            my_dbs=self.client.list_database_names()
            #the database can be acesssed via, client.databasename as below
            self.my_db=self.client.sample_analytics
            self.collections=self.my_db.list_collection_names()
            return self.collections # ['transactions', 'customers', 'accounts']

    def for_each_collection(self):
            collection1=self.collections[0]
            collection2=self.collections[1]
            collection3=self.collections[2]   
            return collection1
            
        
    
        
        




    

connection_str="mongodb+srv://py-trainee:jMaDl9Lyn03r2zT3@trainee-cluster.ojubn5t.mongodb.net/?retryWrites=true&w=majority&appName=trainee-cluster"
obj=Mongo_DB(connection_str)
obj.connection_check()
obj.list_collection_content()

a=obj.list_collection_content()
print(a)




# different connection string can be given by the use of function

# my_db=client.sample_analytics
#         collection1=my_db.accounts
#         # collec2=my_db.customers
#         # collec3=my_db.transactions
#         cursor=collection1.find()
#         print(cursor)
#         for each_col in cursor:
#             print(each_col["products"])