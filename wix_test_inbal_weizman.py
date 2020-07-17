import requests
import ndjson
import json
import datetime
from datetime import date
import pandas_gbq as pg
import pandas as pd
import numpy as np
import time
import mysql.connector
from pandas import json_normalize
from sqlalchemy import create_engine
import csv




def get_upcomings(lat,lon,times):
    url = 'http://api.open-notify.org/iss-pass.json'
    params = {'lat':lat,'lon':lon,'n':times}
    response = response = requests.get(url,params=params)
    your_json = response.content
    parsed = json.loads(your_json.decode('utf-8'))
    df = json_normalize(parsed['response'])
    df['risetime'] = df['risetime'].apply(lambda x:datetime.datetime.fromtimestamp(x))
    return df


def main():
    city_dict ={'Haifa':{'lat':32.809997,'lon':34.987775},'Tel Aviv':{'lat':32.066094,'lon':34.769434},
                'Beer Sheva':{'lat':31.273454,'lon':34.768225}, 'Eilat':{'lat':29.568246,'lon':34.944442}}

    df = pd.DataFrame()
    for key, value in city_dict.items():
       df_temp =  get_upcomings(value['lat'],value['lon'],50)
       df_temp['location_org'] = key
       df_temp['location'] = key.lower().replace(' ','_')
       df = df.append(df_temp).reset_index(drop=True)

    with open('credentials_json.txt') as json_file:
        credentials_dict = json.load(json_file)
    user = credentials_dict['User']
    password = credentials_dict['Password']
    host = credentials_dict['Host']
    database = credentials_dict['Database']

    engine = create_engine("mysql://{}:{}@{}/{}".format(user,password,host,database))
    conn = engine.connect()

    df.to_sql(con=conn, name='orbital_data_inbal_weizman', if_exists='replace')
    new_conn = engine.raw_connection()
    mycursor = new_conn.cursor()
    mycursor.execute("DROP PROCEDURE IF EXISTS `getAvgPerCity`")
    mycursor.execute('''CREATE PROCEDURE getAvgPerCity 
                            (
                               IN city VARCHAR(64),
                                IN des_table VARCHAR(255)
             
                            ) 
                            BEGIN 
                            SET @Sql = CONCAT('INSERT INTO ',des_table,'(location,avg_value,insert_time)
                            select location,avg_per_city, NOW() AS insert_time from
                            (
                             select location,avg(total) as avg_per_city from 
                               (
                            select location,date(risetime) as dt_day,count(*) as total
                            from 
                            orbital_data_inbal_weizman
                            
                            group by location,dt_day
                            ) as q1 
          
                            group by location) as q2
                             WHERE location = "',city,'";');
                             
                            PREPARE stmt FROM @Sql;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;


                            END  ''')
    des_table_arr = []
    mycursor.close()
    mycursor = new_conn.cursor()
    for key in city_dict:
        key = key.lower().replace(' ','_')
        des_table = 'city_stats_' + key + '_inbal_weizman'
        des_table_arr.append(des_table)
        mycursor.execute("DROP TABLE IF EXISTS {};".format(des_table))
        mycursor.execute("CREATE TABLE {} (location VARCHAR(255), avg_value FLOAT, insert_time DATETIME)".format(des_table))
        args = [key,des_table]
        mycursor.callproc('getAvgPerCity', args)
    mycursor.execute('''SELECT * from {}
                        union 
                        SELECT * from {}
                        union 
                        SELECT * from {}
                        union 
                        SELECT * from {}
                        '''.format(des_table_arr[0],des_table_arr[1],des_table_arr[2],des_table_arr[3]))
    result = mycursor.fetchall()
    print(result)
    fp = open('city_stats_inbal_weizman.csv', 'w',newline='')
    myFile = csv.writer(fp)
    header = ['location','average','date']
    myFile.writerow(header)
    myFile.writerows(result)
    fp.close()


main()