from pymongo import MongoClient

class MongoDB:

    def __init__(self, connection_str):
        self.connection = connection_str #connection formed  
    
    def connection_check(self):
        self.client = MongoClient(self.connection)
        if self.client:
            return {"message":"Connected to mongo_db"}
        return {"message":"Not Connected to mongo_db"}


    def list_collection(self):
        if self.client != None:
            # my_dbs=self.client.list_database_names()
            #the database can be acesssed via, client.databasename as below
            self.my_db = self.client.sample_analytics
            self.collections = self.my_db.list_collection_names()
            return self.collections
    
    
    def collection_docs(self):
        if self.client != None:
            self.my_db = self.client.sample_analytics
            def show_documents(collection_name):
                all_data = self.my_db[collection_name]
                documents = []
                for docs in all_data.find({}):
                    documents.append(docs)
                return documents
            collection_docs = []
            for i in self.collections:
                collection_docs.append(show_documents(i))
            return collection_docs

    def collection_name_and_docs(self):
            if self.client != None:
                self.my_db = self.client.sample_analytics
                documents = {}
                def show_documents(collection_name):
                    all_data = self.my_db[collection_name].find({})
                    each_row = []
                    for i in all_data:
                        each_row.append(i)
                        documents[collection_name] = each_row
                    return documents
                collection_docs = []
                for i in self.collections:
                    collection_docs.append(show_documents(i))
                return documents
                

        
            
            
           
            
          
 
        
    
        
        










# different connection string can be given by the use of function

# my_db=client.sample_analytics
#         collection1=my_db.accounts
#         # collec2=my_db.customers
#         # collec3=my_db.transactions
#         cursor=collection1.find()
#         print(cursor)
#         for each_col in cursor:
#             print(each_col["products"])