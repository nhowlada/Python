
# Example python program to read data from a PostgreSQL table and load to pandas DataFrame 
# Clean the data
# and load back into PostgreSQL
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import func, select

createTable = False
truncateTable = True

# Create an engine instance
engine = create_engine('postgresql+psycopg2://postgres:Atnahsinom!1221@localhost:5433/postgres', pool_recycle=3600);

# Connect to PostgreSQL server
try:
    connection = engine.connect();
    print("DB connected!")
except:
    print("DB not connected!")

# Read from PostgreSQL and load into a DataFrame
df = pd.read_sql("select * from cards_ingest.tran_fact", connection);
# pds.set_option('display.expand_frame_repr', False);

# Filling a null value using fillna()
df.fillna('TX', inplace = True)

# Create table
if createTable:
    connection.execute('CREATE TABLE IF NOT EXISTS cards_ingest.tran_fact_stage('
        'tran_id integer,'
        'cust_id varchar(10),'
        'stat_cd varchar(2),'
        'tran_ammt numeric(10,2),'
        'tran_date date,'
        'commission integer);')
# Trauncate table
if truncateTable:
    connection.execute('TRUNCATE TABLE cards_ingest.tran_fact_stage')

# Column added into a DataFrame
df['commission'] = df['tran_ammt']*4

# Insert into a New Table
df = df.to_sql("tran_fact_stage",connection,"cards_ingest",if_exists='append', index=False)

# Initial Table record count
count1 = connection.execute('SELECT COUNT(*) FROM cards_ingest.tran_fact', ).fetchone()
# Stage Table record count
count2 = connection.execute('SELECT COUNT(*) FROM cards_ingest.tran_fact_stage', ).fetchone()
connection.close();

if count1[0]==count2[0]:
    print("Record Count Match!")
else:
    print("Row Count does not Match!")
