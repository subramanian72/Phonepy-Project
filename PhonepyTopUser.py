import json
import pprint
import os
import pandas as pd
import pymysql

tupath = r"D:/Phonepy Prj/pulse-master/data/top/user/country/india/state/"

tuapt_state_list = os.listdir(tupath)
#print(Agg_state_list)

#Aggregted Transaction
clm={'State':[], 'Year':[], 'Quarter':[], 'Topuser_name':[],'Topuser_count':[]}

for i in tuapt_state_list:
    p_i = tupath + i + "/"
    tuAgg_Yr = os.listdir(p_i)
    for j in tuAgg_Yr:  
        p_j = p_i + j + "/"
        tuAgg_Yr_list = os.listdir(p_j)
        for k in tuAgg_Yr_list:
            p_k = p_j + k
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['districts']:
                name = z['name']
                registeredusers = z['registeredUsers']
                clm['Topuser_name'].append(name)
                clm['Topuser_count'].append(registeredusers)               
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))

Topuser_Trans=pd.DataFrame(clm)


# creating column list for insertion
cols = ",".join([str(i) for i in Topuser_Trans.columns.tolist()])

# Connect to the database
connection = pymysql.connect(host='localhost',
                         user='root',
                         password='subbu#123456',
                         db='phonepydb')

# create cursor
cursor=connection.cursor()

# Insert DataFrame recrds one by one.
for i,row in Topuser_Trans.iterrows():
    sql = "INSERT INTO topuser (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()


# Execute query
sql = "SELECT * FROM topuser"
cursor.execute(sql)

# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)


