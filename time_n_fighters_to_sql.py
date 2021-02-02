import sqlalchemy
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import json
import os
import requests
import string
import re

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

## 역대 ufc 경기에 참여했던 인원을 순서대로 list 화 하고, 이의 url 을 담는다.
fighter_url_only_list = []
for red_url, blue_url in fighter_url[['fighter_url_red', 'fighter_url_blue']].itertuples(index=False):
    fighter_url_only_list.append(red_url)
    fighter_url_only_list.append(blue_url)
    fighter_url_only_list = list(dict.fromkeys(fighter_url_only_list))

## ufcstats 웹 사이트에서 선수 목록을 스크래핑한다.
# headers = {'User-agent': 'Mozilla/5.0'}
# url_list = []
# for char in string.ascii_lowercase:
#     url = 'http://www.ufcstats.com/statistics/fighters?char={}&page=all'.format(char)
#     sauce = requests.get(url, headers=headers)
#     soup = BeautifulSoup(sauce.text, 'lxml')
#     tr = soup.find_all('tr', class_='b-statistics__table-row')
#     for r in tr:
#         try:
#             url_list.append(r.find('a').attrs['href'])
#         except AttributeError:
#             continue
#     print(char, 'is done')

## 이제 경기를 했던 선수들의 목록 + 선수 목록을 한다. 당연히 겹치는 건 제외한다.
# fighter_url_only_list = fighter_url_only_list + url_list
# fighter_url_only_list = list(dict.fromkeys(fighter_url_only_list))

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


## fighter Table 을 MYSQL 에 입력한다.
fighter_path = 'fighter_html/'
s = os.listdir(path=fighter_path)
s.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))

fighter_columns = ['fighter_name', 'fighter_nickname', 'height', 'weight',
                   'reach', 'stance', 'dob', 'record_win', 'record_loss',
                   'record_draw', 'record_nc', 'record_sl_pm', 'record_str_acc', 'record_sa_pm',
                   'record_str_def', 'record_td_avg', 'record_td_acc', 'record_td_def', 'record_sub_avg'
                   ]

fighters_df = pd.DataFrame(columns=fighter_columns)
### feet and inches 를 centimeters 로 변환하는 함수를 정의한다.
def to_cm(string):
    import re
    string_split = string.split("'")
    feet = 0
    if len(string_split) > 1:
        feet = re.search(r'\d+', string_split[0]).group()
    try:
        inches = re.search(r'\d+', string_split[-1]).group()
    except:
        return None

    feet = float(feet)
    inches = float(inches)
    centimeters = feet * 30.48 + inches * 2.54

    return centimeters

i = 0
for html in s:
    i += 1
    fighter_soup = BeautifulSoup(open(fighter_path+html), 'html.parser')
    fighter_name = fighter_soup.find('span', class_='b-content__title-highlight').text.strip()
    fighter_nickname = fighter_soup.find('p', class_='b-content__Nickname').text.strip()

    ## physique
    physique_li_list = fighter_soup.find('div', class_='b-list__info-box b-list__info-box_style_small-width js-guide').find_all('li')
    physique_list = []
    for physique in physique_li_list:
        text = physique.find_all(text=True, recursive=False)
        physique_list.append(''.join(t.strip() for t in text))
    ### to be converted into metric
    height = to_cm(physique_list[0])
    ### lbs
    try:
        weight = float(re.search(r'\d+', physique_list[1]).group())
    except:
        weight = None
    ### to be converted into metric
    reach = to_cm(physique_list[2])
    stance = physique_list[3]
    dob = physique_list[4]

    ## record
    record = str.lower(fighter_soup.find('span', class_='b-content__title-record').text.strip())
    record = record.replace('record:', '').strip()
    record_win = record.split('-')[0]
    record_loss = record.split('-')[1]
    record_draw = record.split('-')[2]
    record_nc = 0
    ### record 에 nc 가 있으면 draw, nc 값을 다르게 split 해야 한다.
    draw_nc = record.split('-')[2]
    if draw_nc.find('nc') > -1:
        record_draw = re.findall(r'\d+', draw_nc)[0]
        record_nc = re.findall(r'\d+', draw_nc)[-1]
    # record_win = int(record_win)
    # record_loss = int(record_loss)
    # record_draw = int(record_draw)
    # record_nc = int(record_nc)

    ## career statistics
    stats_ul_list = fighter_soup.find('div', class_='b-list__info-box-left clearfix').find_all('ul')
    stat1_list = []
    for stat in stats_ul_list[0].find_all('li'):
        text = stat.find_all(text=True, recursive=False)
        stat1_list.append(''.join(t.strip() for t in text))
    record_sl_pm = stat1_list[0]
    record_str_acc = stat1_list[1]
    record_sa_pm = stat1_list[2]
    record_str_def = stat1_list[3]

    stat2_list = []
    for stat in stats_ul_list[-1].find_all('li'):
        text = stat.find_all(text=True, recursive=False)
        stat2_list.append(''.join(t.strip() for t in text))
    record_td_avg = stat2_list[1]
    record_td_acc = stat2_list[2]
    record_td_def = stat2_list[3]
    record_sub_avg = stat2_list[4]

    a_row = [fighter_name, fighter_nickname, height, weight,
             reach, stance, dob, record_win, record_loss,
             record_draw, record_nc, record_sl_pm, record_str_acc, record_sa_pm,
             record_str_def, record_td_avg, record_td_acc, record_td_def, record_sub_avg
             ]

    fighters_df_sub = pd.DataFrame([a_row], columns=fighters_df.columns)
    fighters_df = pd.concat([fighters_df, fighters_df_sub])

    print(i,'is done out of', len(s))

fighters_df.reset_index(drop=True, inplace=True)
## data type 을 column 별로 정한다.
### numeric1
fighters_df[['height', 'weight', 'reach']] = fighters_df[['height', 'weight', 'reach']].apply(pd.to_numeric)
### date
fighters_df['dob'] = pd.to_datetime(fighters_df['dob'].str.replace('--', 'NaT'))
### numeric2
fighters_df[['record_win', 'record_loss', 'record_draw', 'record_nc']] = \
    fighters_df[['record_win', 'record_loss', 'record_draw', 'record_nc']].apply(pd.to_numeric)
### numeric3 without percent signs
fighters_df[['record_sl_pm', 'record_sa_pm', 'record_td_avg', 'record_sub_avg']] = \
    fighters_df[['record_sl_pm', 'record_sa_pm', 'record_td_avg', 'record_sub_avg']].apply(pd.to_numeric)
### numeric4 with percent signs
fighters_df[['record_str_acc', 'record_str_def', 'record_td_acc', 'record_td_def']] =  \
    fighters_df[['record_str_acc', 'record_str_def', 'record_td_acc', 'record_td_def']].apply(lambda x: x.str.strip('%')).apply(pd.to_numeric) / 100

fighters_df.to_json('fighter_to_sql.json')