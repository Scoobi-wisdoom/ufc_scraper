import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os
import requests
import string

# MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

# 1. times format 를 DB 에 입력
# times 를 mysql 에 입력하기 위해서 상세 정보를 봐야 한다.
## html 을 저장할 경로 html
path = 'html/'
times_list = list()

i = 0
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
for html_file in s:
    i += 1
    file_soup = BeautifulSoup(open(path + html_file), 'html.parser')
    b_fight_details = file_soup.find_all('i', class_='b-fight-details__label')
    for b in b_fight_details:
        if str(b).find('Time format') > -1:
            times_list.append(b.parent.find_all(text=True, recursive=False)[-1].strip())
            break
    times_list = list(dict.fromkeys(times_list))
    print(i, 'out of', len(os.listdir(path=path)))

times = pd.DataFrame({'times_format': times_list})
## MYSQL 에 데이터를 처음 입력한다: Table times
with engine.connect() as con:
    ## times table
    times.to_sql(con=con, name='times', if_exists='replace', index=True,
                   dtype={
                       None: sqlalchemy.types.INT,
                       'times_format': sqlalchemy.types.VARCHAR(length=255)
                   }
                )
    con.execute('ALTER TABLE `times` ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE `times` CHANGE `index` `time_id` INT;')


# 2. fighters 정보를 ufcstats.com 에서 스크래핑
## 기존에 갖고 있던 matches 관련 html 파일에서 각 fighter 의 url 정보를 얻는다.
fighter_url_columns = ['event_id', 'match_id',
                       'fighter_name_red', 'fighter_nickname_red', 'defeat_red', 'fighter_url_red',
                        'fighter_name_blue', 'fighter_nickname_blue', 'defeat_blue', 'fighter_url_blue'
                       ]
fighter_url = pd.DataFrame(columns=fighter_url_columns)

path = 'html/'
s = os.listdir(path=path)
s.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))
i = 0
for html_file in s:
    i += 1
    event_id = html_file.split('_')[0]
    match_id = html_file.split('_')[-1].split('.')[0]

    file_soup = BeautifulSoup(open(path+html_file), 'html.parser')
    fighters_name_two = file_soup.find_all('a', class_='b-link b-fight-details__person-link')
    fighters_nickname_two = file_soup.find_all('p', class_='b-fight-details__person-title')
    fighters_defeat_two = file_soup.find('div', class_='b-fight-details__persons clearfix').find_all('i')

    fighter_name_red = fighters_name_two[0].text.strip()
    fighter_url_red = fighters_name_two[0].attrs['href']
    ### nickname 이 있는 경우에는 eval 을 적용한다.
    fighter_nickname_red = fighters_nickname_two[0].text.strip()
    if len(fighter_nickname_red) > 0:
        fighter_nickname_red = eval(fighter_nickname_red)
    defeat_red = fighters_defeat_two[0].text.strip()

    fighter_name_blue = fighters_name_two[-1].text.strip()
    fighter_url_blue = fighters_name_two[-1].attrs['href']
    ### nickname 이 있는 경우에는 eval 을 적용한다.
    fighter_nickname_blue = fighters_nickname_two[-1].text.strip()
    if len(fighter_nickname_blue) > 0:
        fighter_nickname_blue = eval(fighter_nickname_blue)
    defeat_blue = fighters_defeat_two[-1].text.strip()

    a_row = [event_id, match_id,
             fighter_name_red, fighter_nickname_red, defeat_red, fighter_url_red,
             fighter_name_blue, fighter_nickname_blue, defeat_blue, fighter_url_blue
             ]
    fighter_url_sub = pd.DataFrame([a_row], columns=fighter_url.columns)
    fighter_url = pd.concat([fighter_url, fighter_url_sub])
    print(i, 'is done out of', len(s))

fighter_url.reset_index(drop=True, inplace=True)
with open('fighter_url.json', 'w') as f:
    f.write(json.dumps(fighter_url.to_dict()))

# 3. fighter_url 에서 defeat 를 통해 results Table 을 만든다.
with open('fighter_url.json', 'r') as f:
    json_data = json.loads(f.read())
    fighter_url = pd.DataFrame(json_data)

results = fighter_url[~fighter_url[['defeat_red', 'defeat_blue']].duplicated(keep='first')][['defeat_red', 'defeat_blue']]
results.reset_index(drop=True, inplace=True)
results['result_r_b'] = results['defeat_red'] + results['defeat_blue']
results['result_r_b'] = results['result_r_b'].apply(lambda x: x[:2])
results = results.drop(['defeat_red', 'defeat_blue'], axis=1)

## MYSQL 에 데이터를 처음 입력한다: Table results
with engine.connect() as con:
    ## results table
    results.to_sql(con=con, name='results', if_exists='replace', index=True,
                     dtype={
                            None: sqlalchemy.types.INT,
                            'result_r_b': sqlalchemy.types.VARCHAR(length=255)
                            }
                   )
    con.execute('ALTER TABLE `results` ADD PRIMARY KEY (`index`);')
    con.execute('ALTER TABLE `results` CHANGE `index` `result_id` INT;')

# 4. fighter_url 을 이용해 fighters Table 을 SQL 에 입력한다.
with open('fighter_url.json', 'r') as f:
    json_data = json.loads(f.read())
    fighter_url = pd.DataFrame(json_data)

fighter_url_only_list = []
for red_url, blue_url in fighter_url[['fighter_url_red', 'fighter_url_blue']].itertuples(index=False):
    fighter_url_only_list.append(red_url)
    fighter_url_only_list.append(blue_url)
    fighter_url_only_list = list(dict.fromkeys(fighter_url_only_list))

## fighter 페이지를 html 파일로 저장한다.
headers = {'User-agent': 'Mozilla/5.0'}
fighter_path = 'fighter_html/'
i = 0
for url in fighter_url_only_list:
    sauce = requests.get(url, headers=headers)
    soup = BeautifulSoup(sauce.text, 'lxml')
    with open(fighter_path+ 'fighter' + '_' + str(i) + '.html', 'w', encoding='utf-8') as f:
        f.write(str(soup))
    i += 1
    print(i, 'is done out of', len(fighter_url_only_list))

for char in string.ascii_lowercase:
    url = 'http://www.ufcstats.com/statistics/fighters?char={}&page=all'.format(char)

# file_soup = BeautifulSoup(open(path + '1.html'), 'html.parser')
# file_soup.find(class_='b-fight-details__persons clearfix').find(class_='b-fight-details__person').find('a').attrs[
#     'href']