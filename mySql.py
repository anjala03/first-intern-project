import mysql.connector

class MySQL_class:
    def __init__(self, host, port, user, password):
        self.my_db=mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password   
        )
        
    def point_connection(self):
        if (mycursor := self.my_db.cursor()):
            self.mycursor = mycursor
            return True
        return False
    
        

obj=MySQL_class(host="127.0.0.1", port="3306",user="root",password="")
print(obj.point_connection())

obj.mycursor.execute("DROP SCHEMA IF EXISTS new;")
    