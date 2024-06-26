from main import mysql_obj
from utils import utility
# Connect to MySQL
mysql_obj.point_connection()
mycursor = mysql_obj.mycursor

data = utility
# Define MySQL schema
TABLES = {}
TABLES['transactions'] = (
    "CREATE TABLE IF NOT EXISTS `transactions` ("
    "  `_id` INT,"
    "  `account_id` INT,"
    "  `transaction_count` INT,"
    "  `bucket_start_date` DATE,"
    "  `bucket_end_date` DATE,"
    "  PRIMARY KEY (`_id`)"
    ")"
)
TABLES['individual_transactions'] = (
    "CREATE TABLE IF NOT EXISTS `individual_transactions` ("
    "  `transaction_id` INT,"
    "  `date` DATE,"
    "  `amount` INT,"
    "  `transaction_code` VARCHAR(255),"
    "  `symbol` VARCHAR(255),"
    "  `price` DECIMAL(20, 10),"
    "  `total` DECIMAL(30, 10)"
    ")"
)
TABLES['customers'] = (
    "CREATE TABLE IF NOT EXISTS `customers` ("
    "  `_id` INT,"
    "  `username` VARCHAR(255),"
    "  `name` VARCHAR(255),"
    "  `address` TEXT,"
    "  `birthdate` DATE,"
    "  `email` VARCHAR(255),"
    "  PRIMARY KEY (`_id`)"
    ")"
)
TABLES['accounts'] = (
    "CREATE TABLE IF NOT EXISTS `accounts` ("
    "  `_id` VARCHAR(255),"
    "  `account_id` INT,"
    "  `limit` INT,"
    "  PRIMARY KEY (`_id`)"
    ")"
)

# Create MySQL tables
for table_name in TABLES:
    try:
        mycursor.execute(TABLES[table_name])
    except Exception as e:
        print(e)

# Migrate data to MySQL
for transaction in data['transactions']:
    # Migrate transactions
    mycursor.execute("INSERT INTO transactions (_id, account_id, transaction_count, bucket_start_date, bucket_end_date) VALUES (%s, %s, %s, %s, %s)",
                         (transaction['_id'], transaction['account_id'], transaction['transaction_count'], transaction['bucket_start_date'], transaction['bucket_end_date']))
    # Migrate individual transactions
    for individual_transaction in transaction['transactions']:
        mycursor.execute("INSERT INTO individual_transactions (transaction_id, date, amount, transaction_code, symbol, price, total) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                             (transaction['_id'], individual_transaction['date'], individual_transaction['amount'], individual_transaction['transaction_code'], individual_transaction['symbol'], individual_transaction['price'], individual_transaction['total']))

for customer in data['customers']:
    # Migrate customers
    mycursor.execute("INSERT INTO customers (_id, username, name, address, birthdate, email) VALUES (%s, %s, %s, %s, %s, %s)",
                         (customer['_id'], customer['username'], customer['name'], customer['address'], customer['birthdate'], customer['email']))

for account in data['accounts']:
    # Migrate accounts
    mycursor.execute("INSERT INTO accounts (_id, account_id, `limit`) VALUES (%s, %s, %s)",
                         (account['_id'], account['account_id'], account['limit']))

# Commit changes and close connections
mysql_obj.my_db.commit()

mycursor.close()

print("Migration completed.")