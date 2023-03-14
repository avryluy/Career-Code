import pyodbc
import pandas as pd

serverMSP = 'SERVERNAME'
db_SF = ""
db_Analytics = ""

def sql_connect(server, db):

    # database = db
    # server = server
    try:
        conn = pyodbc.connect('DRIVER={SQL Server}'';SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=Yes')
        # ';UID=' + username + ';PWD=' + password)
        print("Connection Successful")
    except:
        print("Connection failed")
    


def sql_to_pandas(server, db, query):
    conn = pyodbc.connect('DRIVER={SQL Server}'';SERVER=' + server + ';DATABASE=' + db + ';Trusted Connection=' +"Yes")
    df = pd.read_sql(query, conn)
    print("Data Collected.")
    conn.close()
    return df

