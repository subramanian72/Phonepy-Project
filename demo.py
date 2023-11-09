import streamlit as st
import pandas as pd
import mysql.connector
import numpy as np
import locale
from math import log10
import plotly.express as px
import json

col1,col2 = st.columns(2)

locale.setlocale(locale.LC_MONETARY, 'en_IN')

#To establish a connection to mysql
mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="subbu#123456",
            database="phonepydb"
            )

mycursor = mydb.cursor()

#To print with indian format
def formatINR(number):
    if number < 0 and number > -1000:
        return number
    else:
        s, *d = str(number).partition(".")
        r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
        return "".join([r] + d)
   
#formats the float value to indian money format
def convertINR(value):    
    MAP = {
        0: '',
        1: '',
        2: '',
        3: 'Thousand',
        4: 'Thousand',
        5: 'Lakh',
        6: 'Lakh',
        7: 'Cr',
        8: 'Cr',
    }

    power = int(log10(abs(value)))
    locale.setlocale(locale.LC_ALL, 'en_IN')
    if power <= 8:

        humanized = locale.format('%d', value, grouping=True)
        rounded = humanized[:4].replace(',', '.')
        s = rounded + ' ' + MAP[power]
        if (rounded != '1.00'): s = s + 's'
    else:
        value = value / 10000000
        humanized = locale.format('%d', value, grouping=True)
        s = humanized + ' ' + 'Crs'

    return s

with col2:
    def main():
        st.sidebar.header('Choose your filter:')
        agg_user = st.sidebar.selectbox("Select an option",['Aggregated','User'])
        year = st.sidebar.selectbox("Select the year",['2018','2019','2020','2021','2022','2023'])
        quarters = st.sidebar.selectbox("Select the quarter",['Q1(Jan-Mar)','Q2(Apr-Jun)','Q3(Jul-Sep)','Q4(Oct-Dec)'])

        if quarters == "Q1(Jan-Mar)":
            qur="1"
        elif quarters == "Q2(Apr-Jun)":
            qur = "2" 
        elif quarters == "Q3(Jul-Sep)":
            qur = "3"        
        else:
            qur = "4"

        if agg_user == "Aggregated":            
                select_smt = "select Year, Quarter, Sum(Transaction_count) as Transactioncnt, Sum(Transaction_amount) as Transactionamount, avg(Transaction_amount) as Avgamount from aggregatetrans where year = %(year)s and quarter in (%(quarter)s) group by year, Quarter"
                mycursor.execute(select_smt,{'year':year, 'quarter': qur})
                result = mycursor.fetchall()

                # df = pd.DataFrame(mycursor.fetchall(), columns = ['Year', 'Quarter','State', 'Transactioncnt','Transactionamount','Avgamount'])
                # st.write(df)

                st.write("<h3> Transactions: </h3>", unsafe_allow_html=True)

                for row in result:
                    st.write("<B>All PhonePe transactions (UPI + Cards + Wallets) : </B>", formatINR(row[2]) , unsafe_allow_html=True)
                    st.write("<B>Total payment value:</B>", convertINR(row[3]), unsafe_allow_html=True)
                    # st.write("<B>Avg. transaction value:</B>", locale.currency(row[4] ,grouping=True), unsafe_allow_html=True)

                select_state =  "select Year, Quarter, Transaction_type, sum(Transaction_count) as Transactioncnt from aggregatetrans where year = %(year)s and quarter in (%(quarter)s) group by year, Quarter, Transaction_type"
                mycursor.execute(select_state,{'year':year, 'quarter': qur})
                result1 = mycursor.fetchall()   

                st.write("<h3> Categories: </h3>", unsafe_allow_html=True)            

                for strow in result1:                 
                    st.write("<B>", strow[2], "</B> : " , convertINR(strow[3]),  unsafe_allow_html=True)

        elif agg_user == "User":
            select_smt = "select Year, Quarter, Sum(User_count) as Usercnt from aggregateuser where year = %(year)s and quarter in (%(quarter)s) group by year, Quarter"
            mycursor.execute(select_smt,{'year':year, 'quarter': qur})
            result = mycursor.fetchall()

            st.write("<h3> Users: </h3>", unsafe_allow_html=True)
            for row in result:
                st.write("<B>Registered PhonePe users : </B>", formatINR(row[2]) , unsafe_allow_html=True)

            select_state =  "select Year, Quarter, State, sum(User_count) as Usercnt from aggregateuser where year = %(year)s and quarter in (%(quarter)s) group by year, Quarter, State"
            mycursor.execute(select_state,{'year':year, 'quarter': qur})
            result1 = mycursor.fetchall()   

            st.write("<h3> States: </h3>", unsafe_allow_html=True)            

            for strow in result1:                 
                    st.write("<B>", strow[2], "</B> : " , convertINR(strow[3]),  unsafe_allow_html=True)


    # df = pd.read_csv("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/active_cases_2020-07-17_0800.csv")

with col1:
    st.write('<h1 style="color:purple;"> Phonepe Plus Data visualization</h1>', unsafe_allow_html=True)
    india_states = json.load(open("states_india.geojson",'r'))
    state_id_map={}
    for feature in india_states['features']:
        feature['id'] = feature['properties']['state_code']
        state_id_map[feature['properties']['st_nm']] = feature['id']

    df = pd.read_csv('India_census1.csv')
    df['Density'] = df['Population Density[a]'].apply(lambda x: int(x.split("/")[0].replace(",","")))
    df['id'] = df['State or Union Territory'].apply(lambda x:state_id_map[x])

    df['Densityscale'] = np.log10(df["Density"])

    fig = px.choropleth(
                        df,
                        locations='id', 
                        geojson=india_states,        
                        hover_name='State or Union Territory',
                        hover_data="Density",
                        color='Densityscale',
                        color_continuous_scale='Reds'
                        )

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig)
    # fig.show()

if __name__ == "__main__":
    main()

