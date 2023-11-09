import json
import pprint
import os
import pandas as pd
import pymysql

mupath = r"D:/Phonepy Prj/pulse-master/data/map/user/hover/country/india/state/"

muapt_state_list = os.listdir(mupath)


#Map user
clm={'State':[], 'Year':[], 'Quarter':[], 'Mapuser_district':[], 'Mapuser_count':[]}

for i in muapt_state_list:
    p_i = mupath + i + "/"
    muAgg_Yr = os.listdir(p_i)
    for j in muAgg_Yr:  
        p_j = p_i + j + "/"
        muAgg_Yr_list = os.listdir(p_j)
        for k in muAgg_Yr_list:
            p_k = p_j + k
            Data = open(p_k,'r')
            D = json.load(Data)
            S = D['data']['hoverData']
            for key,v in S.items():
                 name = key
                 count=str(v['registeredUsers'])                
                 clm['Mapuser_district'].append(name)
                 clm['Mapuser_count'].append(count)                
                 clm['State'].append(i)
                 clm['Year'].append(j)
                 clm['Quarter'].append(int(k.strip('.json')))

Mapuser_Trans=pd.DataFrame(clm)


# creating column list for insertion
cols = ",".join([str(i) for i in Mapuser_Trans.columns.tolist()])

# Connect to the database
connection = pymysql.connect(host='localhost',
                         user='root',
                         password='subbu#123456',
                         db='phonepydb')

# create cursor
cursor=connection.cursor()

# Insert DataFrame recrds one by one.
for i,row in Mapuser_Trans.iterrows():
    sql = "INSERT INTO mapuser (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()


# Execute query
sql = "SELECT * FROM mapuser"
cursor.execute(sql)

# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)


