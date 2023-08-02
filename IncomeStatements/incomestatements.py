from credential import password
from sqlalchemy import create_engine
import urllib
import pandas as pd
import requests
import json

driver = '{ODBC Driver 18 for SQL Server}'
server = 'ethanbauer-server-prod-001.database.windows.net'
database = 'ethanbauerDB-prod-001'
user = 'cloudadmin'
conn = F"""Driver={driver};Server=tcp:{server},1433;Database={database};
Uid={user};Pwd={password};Encryption=yes;TrustServerCertificate=no;Connection Timeout=30;"""



params = urllib.parse.quote_plus(conn)
conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
engine = create_engine(conn_str, fast_executemany=True)

df = pd.read_sql("SELECT * FROM [alpha_vantage].[stock_list] WHERE [assetType] = 'Stock'", conn_str)
print(df)

for symbol in df.symbol:
    url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey=BURCSC12I2PSYHNS'
    r = requests.get(url)
    jsonR = json.loads(r.text)
    try:
        data = jsonR['quarterlyReports']
        df = pd.read_json(json.dumps({i: data[i] for i in range(len(data))}), orient='index')
        df.to_sql('income_statements_stage', schema='alpha_vantage_stage', con=conn_str, if_exists='append', index=False, method='multi', chunksize=32)
        print("SUCCESS")
    except:
        print('data not available for' + symbol)
    

# print(dir(engine))



# cursor.execute("""
#                DECLARE @RC int
#                EXECUTE @RC = [alpha_vantage_stage].[sp_merge_income_statements]
#                """)
