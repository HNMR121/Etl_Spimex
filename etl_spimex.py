import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime as dt
import sqlalchemy

url_spimex = 'https://spimex.com'
# name_xls = '/upload/reports/oil_xls/oil_xls_20210312162000.xls?r=1370'
url_parse = 'https://spimex.com/markets/oil_products/trades/results/'

engine = sqlalchemy.create_engine("mssql+pyodbc://@{sds-srv-dev}/{Spimex2}?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes")
sql = """SELECT TOP(1) Дата
FROM            dbo.SPB
GROUP BY Дата, Показатель
HAVING        (Показатель = N'Нефтепродукты')
ORDER BY Дата DESC"""


class EtlSpimex:

    def __init__(self, ):
        # self.name_xls2 = self.date_check()
        # self.name_xls = self.parse_name_xls()
        # self.soup = BeautifulSoup(self.get_response(url_parse).text, "lxml")
        self.date_last = self.last_date_base()


    # def read_xls(self):
    #     return  pd.read_excel(f'{url_spimex}{name_xls}')

    def get_response(self,url_parse):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'}
        response = requests.get(url_parse,headers=headers)
        return response

    def get_soup(self,url_parse):
        soup = BeautifulSoup(self.get_response(url_parse).text, "lxml")
        return soup

    def parse_name_xls(self):
        soup = self.get_soup(url_parse)
        # soup = self.get_soup(self,url_parse)
        z = soup.find_all('a', {"class": "accordeon-inner__item-title link xls"}, href=True)
        name_xls = []
        for a in z:
            if a.text:
                if len(a['href']) > 13:
                    name_xls.append(a['href'])
        return name_xls

    def last_date_base(self):
        date_last = pd.read_sql(sql, engine)
        return date_last




    def date_check(self,):
        name_xls2 = []
        for a in self.parse_name_xls():
            year = a.split('xls_')[1][:4]
            month = a.split('xls_')[1][4:6]
            day = a.split('xls_')[1][6:8]
            date_parse = f'{day}-{month}-{year}'
            date_parse = dt.datetime.strptime(date_parse, '%d-%m-%Y').replace(hour=0, minute=0, second=0, microsecond=0)
            date_df = self.date_last.iloc[0].to_string().split("   ")[1]
            date_df = dt.datetime.strptime(date_df, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
            if date_parse > date_df:
                name_xls2.append(a)
        return name_xls2








    def etl_df(self,df):
        self.df = df
        date = self.df['Форма СЭТ-БТ'][2].split(': ')[1]
        collist = ['Код', 'Дата', 'SKU', 'Базис', 'Объем Договоров,т', 'Объем Договоров,руб', 'Цена Минимальная,руб',
                   'Цена Средневзвешенная,руб', 'Цена Максимальная,руб', 'Цена Рыночная,руб',
                   'Цена в Заявках лучшее предложение,руб', 'Цена в Заявках лучший спрос,руб', 'Кол-во договоров,шт',
                   'Показатель']
        df = df.drop(columns=['Unnamed: 0', 'Unnamed: 6', 'Unnamed: 7'])
        df = df.iloc[7:]
        df = df.rename(columns={'Форма СЭТ-БТ': collist[0],
                                  'Unnamed: 2': collist[2],
                                  'Unnamed: 3': collist[3],
                                  'Unnamed: 4': collist[4],
                                  'Unnamed: 5': collist[5],
                                  'Unnamed: 8': collist[6],
                                  'Unnamed: 9': collist[7],
                                  'Unnamed: 10': collist[8],
                                  'Unnamed: 11': collist[9],
                                  'Unnamed: 12': collist[10],
                                  'Unnamed: 13': collist[11],
                                  'Unnamed: 14': collist[12]

                                  })
        df['Дата'] = date
        df['Показатель'] = 'Нефтепродукты'
        df = df.reindex(columns=collist)
        df = df[df['SKU'].notnull()]
        return df




    def read_xls2(self):
        df = self.etl_df(pd.read_excel(f'{url_spimex}'+ self.date_check()[0]))
        # df = pd.read_excel(f'{url_spimex}'+ self.name_xls[0])
        for i in self.date_check():
            df2 = self.etl_df(pd.read_excel(url_spimex + i))
            df = pd.concat([df, df2], ignore_index=True)

        return df


    def load_to_db(self):
        df = self.read_xls2()
        df.to_sql('SPB', con=engine, if_exists='append', index=False)
        print("ok")






etl = EtlSpimex()
df = etl.read_xls2()
print(df)
# etl.load_to_db()
# df = etl.parse_name_xls()
# df = etl.date_check()
# df = etl.read_xls2()
# print(df)
# df.to_excel(r'C:\Users\a.fadeev\PycharmProjects\Samples\tp\Test.xlsx',sheet_name="Лист1",index=False)
# print(df2)
# df2.to_excel(r'C:\Users\a.fadeev\PycharmProjects\Samples\tp\Test.xlsx',sheet_name="Лист1",index=False)

# print(etl.last_date_base())

# print(etl.date_check())

# df2 = etl.read_xls2()
# print(df2)

# Столбец даты
# date_df = '01.01.2021'
# date_df = dt.datetime.strptime(date_df,'%d.%m.%Y').replace(hour=0, minute=0, second=0, microsecond=0)