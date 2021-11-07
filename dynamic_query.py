import pymysql

# Connection params
user = 'admin'
password = 'awspassword'
host = 'database-1.#####.us-east-2.rds.amazonaws.com'
port = 3306
db = 'test_db'

# Launch connection outside lambda handler to prevent repeated connection launches
con = pymysql.connections.Connection(host=host, user=user, passwd=password, db=db, port=port)

# Exmaple Dynamic SQL query generator
# To be called inside the lambda fucntion. Pass params through event object in handler params
def display_columns(cols, target_table):

    return (f'''SELECT {','.join([col for col in cols])}
            
            FROM {target_table};
            
            ''')


def lambda_handler(event, context):
    
    cursor = con.cursor()
    
    # Execute query
    # Params can be passed through the event object during trigger
    cursor.execute(display_columns('col_list', 'db.table'))

    # Print return to console
    return '\n'.join([row for row in cursor.fetchall()])





