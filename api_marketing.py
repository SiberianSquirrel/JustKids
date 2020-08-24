#!/usr/bin/env python
# coding: utf-8


import threading
import pymysql
import json
import argparse
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights



#%%
#------Создание модуля для запуски из команндой строки-------#
parser = argparse.ArgumentParser(description = 'Запуск генерации отчета из API MARKETNG Facebook')
parser.add_argument('access_token', type = str, help='Токен доступа для  API MARKETNG Facebook')
parser.add_argument('app_id', type = str, help = 'ID приложения ')
parser.add_argument('app_secret', type = str, help = 'Секрет приложения (приватная информация)')
parser.add_argument('user_db', type = str, help = 'Пользователь для подключения к MYSQL серверу ')
parser.add_argument('password_db', type = str,help = 'Пароль для подключения к  к MYSQL серверу')
parser.add_argument('--host_db', type = str, default='localhost',help = 'Хост для подключения к MYSQL серверу')
parser.add_argument('--name_db', type = str,default='db_app' , help = 'Названия создаваемой базы для хранения отчетов')
parser.add_argument('--time_per_request', type = int, default=60,help = 'Время между запросами к API MARKETNG Facebook (в секундах)')
args = parser.parse_args() # parsing getting arguments

#%%
#-------Иницилизация основных переменных-------#
access_token = args.access_token
app_id = args.app_id
app_secret=args.app_secret
user_db = args.user_db
password_db = args.password_db
host_db = args.host_db
name_db = args.name_db
time_per_request=args.time_per_request

print(user_db,password_db,host_db,name_db,time_per_request)


#%%


class MYSQL_DB(object):

    def __init__(self,user,password,host,database):
        #иницилизация параметров для подключения к серверу MY SQL
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        
     #magic method для работы с контекстным менеджером
    def __enter__(self):
        self.db_config={'host':self.host,'password':self.password,'user':self.user,'autocommit':True}
        self.conn=pymysql.connect(**self.db_config) # create connection
        print('Подключение к  MY SQL server прошло успешно ')
        return self
    
    # декоратор для функции которой нужно выполнить запрос через курсор ( без использования fetch )
    def with_cursor(f):
        def with_cursor(self,*args):
            cursor=self.conn.cursor()
            #args=tuple([item for item in args])
            try:
                f(self,cursor,*args)
            except (Exception, pymysql.Error) as error :
                print ("Возникла ошибка при подключении к  MYSQL", error)
            finally:
                cursor.close()
        return with_cursor
    
    
    #magic method для работы с контекстным менеджером
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        print('Подключение к   MYSQL server закрыто')
    
     #-------CRUD операции -------#

    # создание базы данных и таблицы для записи в нее данных
    @with_cursor
    def create_storage(self,cursor,*args):
        create_db=f'''CREATE DATABASE IF NOT EXISTS {self.database}'''
        create_table=f'''CREATE TABLE IF NOT EXISTS {self.database}.facebook_insights 
                        (
                             time_request datetime
                            ,link_click int
                            ,view int
                            ,cost_per_action decimal(15,2)
                        )
                        '''
        for sql in [create_db,create_table]:
            cursor.execute(sql)
        print (f"Успешно создана таблица facebook_insights  в базе данных {self.database}")
      
    # запись полученных данных из API в таблицу   
    @with_cursor
    def insert_campaign_info(self,cursor,*args):
        insert_records=f"""INSERT INTO {self.database}.facebook_insights (time_request, link_click, view, cost_per_action) 
                                VALUES ('{args[0]}', {args[1]}, {args[2]}, {args[3]});"""

        print(f"Прошла вставка данных с временной меткой {args[0]} в таблицу facebook_insights""")
        cursor.execute(insert_records)
    
    


#%%
class API_Fb(object):

    def __init__(self, app_id, app_secret, access_token):
        FacebookAdsApi.init(app_id, app_secret, access_token)
        print('Приложение успешно иницилизировано')

        
        me = me = AdAccountUser(fbid='me')
        self.my_account = me.get_ad_accounts()[0]

    # формирование отчета о кампании аккаунта
    def campaign_reports(self, since, until):
        params={  'time_range': {'since':since,'until':until},'level': 'ad'}
        #https://developers.facebook.com/docs/marketing-api/reference/ads-insights/?locale=ru_RU
        fields =  [#AdsInsights.Field.account_id,
                   #AdsInsights.Field.account_name,
                   #AdsInsights.Field.campaign_id,
                   #AdsInsights.Field.campaign_name,
                 
                AdsInsights.Field.unique_clicks # клики
               ,AdsInsights.Field.full_view_impressions # показы
               ,AdsInsights.Field.cost_per_unique_click # затраты
                ]

        api_answer = self.my_account.get_insights(params=params, fields=fields)
        return api_answer


#%%

def main():
    with  MYSQL_DB(user_db,password_db,host_db,name_db) as db:
        db.create_storage()
        api_fb=API_Fb(app_id, app_secret, access_token)
  
        
        dt=datetime.now()
        time_request=dt.strftime("%Y-%m-%d %H:%M:%S")
        date_request=dt.strftime("%Y-%m-%d")
        print(time_request)
        
        api_report=api_fb.campaign_reports('2020-01-01',date_request)
        # api_report = json.loads(api_report)
        if len(api_report)==0:
            db.insert_campaign_info(time_request,0,0,0)
        else:
            db.insert_campaign_info(time_request
                                    ,api_report['unique_clicks']
                                    ,api_report['full_view_impressions']
                                    ,api_report['cost_per_unique_click'])
        threading.Timer(time_per_request, main).start() # время в секундах


# In[22]:

if __name__ == "__main__":
    main()






