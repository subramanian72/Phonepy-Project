import json
import pprint
import os
import pandas as pd
from pprint import pprint
import pymysql
#Aggregated Users

upath = r"D:/Phonepy Prj/pulse-master/data/aggregated/user/country/india/state/"
Agg_userstate_lst = os.listdir(upath)

""" Data = open(upath,'r')
D = json.load(Data)

print(D) """

clm={'State':[], 'Year':[], 'Quarter':[],'User_brand':[],'User_count':[],'User_percentage':[] }
for i in Agg_userstate_lst:
    p_i = upath + i + "/"
    Aggu_Yr = os.listdir(p_i)
    for j in Aggu_Yr:  
        p_j = p_i + j + "/"
        Aggu_Yr_list = os.listdir(p_j)
        for k in Aggu_Yr_list:
            p_k = p_j + k
            Data = open(p_k,'r')
            D = json.load(Data)
            # print(D)
            # if D is not None:
            for z in D['data']['usersByDevice'] or []:
                # pprint(z)
                
                #userid = z['registeredUsers'][0]
                brand = z['brand']
                count=z['count']
                percentge=z['percentage']
                
                clm['User_brand'].append(brand)
                clm['User_count'].append(count)
                clm['User_percentage'].append(percentge)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))

Agg_User=pd.DataFrame(clm)

# creating column list for insertion
cols = ",".join([str(i) for i in Agg_User.columns.tolist()])

# Connect to the database
connection = pymysql.connect(host='localhost',
                         user='root',
                         password='subbu#123456',
                         db='phonepydb')

# create cursor
cursor=connection.cursor()

# Insert DataFrame recrds one by one.
for i,row in Agg_User.iterrows():
    sql = "INSERT INTO aggregateuser (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()


# Execute query
sql = "SELECT * FROM aggregateuser"
cursor.execute(sql)

# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)
