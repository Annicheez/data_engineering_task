import pandas as pd
import os
import pymysql
from sqlalchemy import create_engine

# Parent directory containing all json files
parent_dir = 'path_to_directory'

# Iterate through recurisvely to identify relevant files
files = [os.path.join(root, name) for root, direc, files in os.walk(parent_dir) \
    for name in files if name.split('.')[-1] == 'gz'] 

# Initalise master dataframe to append to and injest into the RDS
# Concactenating dataframes locally using pandas to ensure 0% data loss
master_df = pd.DataFrame()

for file in files:
    
    master_df = master_df.append(pd.read_json(file, lines=True))


# Converting pandas object types to string for safe injestion of non-sql dtypes
master_df = master_df.astype(
    {col: 'str' for col, dtype in master_df.dtypes.to_dict().items() if dtype == 'object'}
)

# RDS connection params
user = 'admin'
password = 'awspassword'
host = 'database-1.######.us-east-2.rds.amazonaws.com'
port = '3306'
db = 'test_db'

# Create connection string and launch connection
db_connection_str = f'mysql+pymysql://{user}:{password}@{host}/{db}'

con = create_engine(db_connection_str).connect()

# Write master file to a table in the db
# Setting chunksize to increase insert more than one row in a single insert statement
master_df.to_sql(con=con, name='tweets_data', 
                if_exists='append', 
                chunksize=50000,
                index=False)



