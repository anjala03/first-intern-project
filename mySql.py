import mysql.connector

class MySQL_class:
    def __init__(self, host, port, user):
        self.my_db=mysql.connector.connect(
            host=host,
            port=port,
            user=user, 
            database="sample_analytics"
        )
        
    def point_connection(self):
        if (mycursor := self.my_db.cursor()):
            self.mycursor = mycursor
            return True
        return False

    

    def close_connection(self):
        if self.mycursor.close():
            print("cursor closeddd")
            if self.my_db.close():
                print("db closed")
                return True
            return False


# obj.mycursor.execute("DROP SCHEMA IF EXISTS new;")
    