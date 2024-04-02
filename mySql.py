import mysql.connector

class MySQL_class:
    def __init__(self, host, port, user, password):
        self.my_db=mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password, 
            database="sample_analytics"
        )
        
    def point_connection(self):
        if (mycursor := self.my_db.cursor()):
            self.mycursor = mycursor
            return True
        return False
    

    def make_schema(self):
        self.mycursor.execute("CREATE TABLE accounts")
        self.mycursor.execute("CREATE TABLE customers")
        self.mycursor.execute("CREATE TABLE transactions")

        return 
# obj.mycursor.execute("DROP SCHEMA IF EXISTS new;")
    