import json
import pprint
import os
import pandas as pd
import pymysql

mpath = r"D:/Phonepy Prj/pulse-master/data/map/transaction/hover/country/india/state/"

mapt_state_list = os.listdir(mpath)

#Aggregted Transaction
clm={'State':[], 'Year':[], 'Quarter':[], 'Map_type':[],'Map_count':[],'Map_amount':[] }

for i in mapt_state_list:
    p_i = mpath + i + "/"
    mAgg_Yr = os.listdir(p_i)
    for j in mAgg_Yr:  
        p_j = p_i + j + "/"
        mAgg_Yr_list = os.listdir(p_j)
        for k in mAgg_Yr_list:
            p_k = p_j + k
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['hoverDataList']:
                name = z['name']
                count=z['metric'][0]['count']
                amount=z['metric'][0]['amount']
                clm['Map_type'].append(name)
                clm['Map_count'].append(count)
                clm['Map_amount'].append(amount)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))

Map_Trans=pd.DataFrame(clm)

# creating column list for insertion
cols = ",".join([str(i) for i in Map_Trans.columns.tolist()])

# Connect to the database
connection = pymysql.connect(host='localhost',
                         user='root',
                         password='subbu#123456',
                         db='phonepydb')

# create cursor
cursor=connection.cursor()

# Insert DataFrame recrds one by one.
for i,row in Map_Trans.iterrows():
    sql = "INSERT INTO maptrans (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()


# Execute query
sql = "SELECT * FROM maptrans"
cursor.execute(sql)

# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)

