import json
import pprint
import os
import pandas as pd
import pymysql

tpath = r"D:/Phonepy Prj/pulse-master/data/top/transaction/country/india/state/"

tapt_state_list = os.listdir(tpath)

#Top Transaction
clm={'State':[], 'Year':[], 'Quarter':[], 'Top_type':[],'Top_count':[],'Top_amount':[] }

for i in tapt_state_list:
    p_i = tpath + i + "/"
    tAgg_Yr = os.listdir(p_i)
    for j in tAgg_Yr:  
        p_j = p_i + j + "/"
        tAgg_Yr_list = os.listdir(p_j)
        for k in tAgg_Yr_list:
            p_k = p_j + k
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['districts']:
                name = z['entityName']
                count=z['metric']['count']
                amount=z['metric']['amount']
                clm['Top_type'].append(name)
                clm['Top_count'].append(count)
                clm['Top_amount'].append(amount)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))

Top_Trans=pd.DataFrame(clm)


# creating column list for insertion
cols = ",".join([str(i) for i in Top_Trans.columns.tolist()])

# Connect to the database
connection = pymysql.connect(host='localhost',
                         user='root',
                         password='subbu#123456',
                         db='phonepydb')

# create cursor
cursor=connection.cursor()

# Insert DataFrame recrds one by one.
for i,row in Top_Trans.iterrows():
    sql = "INSERT INTO toptrans (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()


# Execute query
sql = "SELECT * FROM toptrans"
cursor.execute(sql)

# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)




