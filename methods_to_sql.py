import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os


# . MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))


# 데이터 불러오기
with open('fight_list_url.json', 'r') as f:
    json_data = json.loads(f.read())

fight_list_url = pd.DataFrame(json_data)

methods = fight_list_url[~fight_list_url['Method'].apply(lambda x: x.split(' ')[0]).duplicated(keep='first')]['Method']
methods = pd.DataFrame(methods.apply(lambda x: x.split(' ')[0]))
methods.reset_index(drop=True, inplace=True)

# methods 를 mysql 에 입력하기 위해서 상세 정보를 봐야 한다.
## html 을 저장할 경로 html
path = 'html/'
test_dict = dict()
for m in methods['Method']:
    test_dict[m] = set()

s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
for html_file in s:
    start = html_file.find('_')
    end = html_file.find('.')
    match_id = int(html_file[start + 1: end])
    m = fight_list_url.iloc[match_id,:]['Method'].split(' ')[0]
    file_soup = BeautifulSoup(open(path+html_file), 'html.parser')
    M = file_soup.find('i', {'style': 'font-style: normal'}).text
    test_dict[m].add(M.strip())
    print( match_id + 1,'out of' ,len(os.listdir(path=path)))

# KO/TKO 를 제외한 모든 method 는 일대일 대응이다. KO/TKO 관련 row 를 추가하자.
ko_index = methods[methods == 'KO/TKO'].dropna().index
methods_split1 = methods.loc[0:ko_index[0]]
methods_split2 = methods.loc[methods.index.difference(methods_split1.index)]

## KO/TKO 관련 row 를 추가
df_ko = pd.DataFrame(['KO/TKO'], columns=methods_split1.columns)
methods_split1 = pd.concat([methods_split1, df_ko])
methods = pd.concat([methods_split1, methods_split2])
methods.reset_index(drop=True, inplace=True)

methods_M = list()
for m in methods['Method']:
    methods_M.append(test_dict[m].pop())
methods['method_long'] = methods_M
## column name 변경
methods = methods.rename({'Method': 'method'}, axis=1)

## MYSQL 에 데이터를 처음 입력한다: Table methods
with engine.connect() as con:
    ## methods table
    methods.to_sql(con=con, name='methods', if_exists='replace', index=True,
                     dtype={
                            None: sqlalchemy.types.INT,
                            'method': sqlalchemy.types.VARCHAR(length=255),
                            'method_long': sqlalchemy.types.VARCHAR(length=255)
                            }
                   )
    con.execute('ALTER TABLE `methods` ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE `methods` CHANGE `index` `method_id` INT;')