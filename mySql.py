import mysql.connector

class MySQLclass:
    def __init__(self, host, port, user):
        self.my_db=mysql.connector.connect(
            host = host,
            port = port,
            user = user, 
            database = "sample_analytics"
        )
        
    def point_connection(self):
        if (mycursor := self.my_db.cursor(buffered = True)):
            self.mycursor = mycursor
            return True
        return False
   

    def close_connection(self):
        if self.mycursor:
            self.mycursor.close()
        if self.my_db:
            self.my_db.close()
            return {"db_close": True}
        else:
            return {"db_close": False}
        

    


# obj.mycursor.execute("DROP SCHEMA IF EXISTS new;")
    