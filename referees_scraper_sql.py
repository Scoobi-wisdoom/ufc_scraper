import os
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
import pymysql

# . MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

referee_name = []

path = 'html/'
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
i = 0
for html in s:
    i += 1
    soup = BeautifulSoup(open(path+html), 'html.parser')
    referee = soup.find('p', class_='b-fight-details__text').find('span').text.strip()
    referee_name.append(referee)
    print(i, 'is done out of', len(s))

referee_name = list(dict.fromkeys(referee_name))
referees = pd.DataFrame({'referee_name': referee_name})

## MYSQL 에 데이터를 처음 입력한다: Table referees
with engine.connect() as con:
    ## methods table
    referees.to_sql(con=con, name='referees', if_exists='replace', index=True,
                     dtype={
                            None: sqlalchemy.types.INT,
                            'referee_name': sqlalchemy.types.VARCHAR(length=255)
                            }
                   )
    con.execute('ALTER TABLE `referees` ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE `referees` CHANGE `index` `referee_id` INT;')