import json
import pprint
import os
import pandas as pd
import pymysql

path = r"D:/Phonepy Prj/pulse-master/data/aggregated/transaction/country/india/state/"


#path="/pulse-master/data/aggregated/transaction/country/india/state"
Agg_state_list = os.listdir(path)
# Agg_state_list1 = list(map(lambda x: x.replace("-"," "), Agg_state_list))

# print(Agg_state_list1)

#Aggregted Transaction
clm={'State':[], 'Year':[], 'Quarter':[], 'Transaction_type':[],'Transaction_count':[],'Transaction_amount':[] }

for i in Agg_state_list:
    p_i = path + i + "/"
    Agg_Yr = os.listdir(p_i)
    for j in Agg_Yr:  
        p_j = p_i + j + "/"
        Agg_Yr_list = os.listdir(p_j)
        for k in Agg_Yr_list:
            p_k = p_j + k
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['transactionData']:
                name = z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']
                clm['Transaction_type'].append(name)
                clm['Transaction_count'].append(count)
                clm['Transaction_amount'].append(amount)
                clm['State'].append(list(map(lambda x: x.replace("-"," "),i)))
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))

Agg_Trans=pd.DataFrame(clm)
print(Agg_Trans)

# # creating column list for insertion
# cols = ",".join([str(i) for i in Agg_Trans.columns.tolist()])

# # Connect to the database
# connection = pymysql.connect(host='localhost',
#                          user='root',
#                          password='subbu#123456',
#                          db='phonepydb')

# # create cursor
# cursor=connection.cursor()

# # Insert DataFrame recrds one by one.
# for i,row in Agg_Trans.iterrows():
#     sql = "INSERT INTO aggregatetrans (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
#     cursor.execute(sql, tuple(row))

#     # the connection is not autocommitted by default, so we must commit to save our changes
#     connection.commit()

# # Execute query
# sql = "SELECT * FROM aggregatetrans"
# cursor.execute(sql)

# # Fetch all the records
# result = cursor.fetchall()
# for i in result:
#     print(i)


