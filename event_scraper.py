from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from time import sleep
import json

## 1. 기본 스크래핑. 데이터 없음. 이벤트가 새로 열릴 때마다 해야 함
headers = {'User-agent': 'Mozilla/5.0'}
events_list = []
url_list = []

url = "http://www.ufcstats.com/statistics/events/completed?page=all"
sauce = requests.get(url, headers=headers)
soup = BeautifulSoup(sauce.text, 'lxml')
event_list = []
date_list = []
location_list = []
url_list = []
for tr in soup.find("tbody").find_all("tr", class_="b-statistics__table-row"):
    try:
        event_list.append(tr.find("a").text.strip())
        date_list.append(tr.find("span").text.strip())
        location_list.append(
            tr.find("td", class_="b-statistics__table-col b-statistics__table-col_style_big-top-padding").text.strip())
        url_list.append(tr.find("a").attrs["href"])
    except AttributeError:
        pass

events_url = pd.DataFrame({"event": event_list, "date": date_list, "location": location_list, "url": url_list})
events_url["date"] = pd.to_datetime(events_url["date"])
events_url.sort_index(inplace=True, ascending=False)
events_url.reset_index(drop=True, inplace=True)

events_no_url = events_url.drop(["url"], axis=1)

# 2. MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

## MYSQL 에 데이터를 처음 입력한다.
# with engine.connect() as con:
#     events_no_url.to_sql(con=con, name='events', if_exists='replace', index=True)
#     con.execute('ALTER TABLE `events` ADD PRIMARY KEY (`index`);')

## MYSQL DB 와 스크래핑 데이터를 비교한다.
with engine.connect() as con:
    events_no_url_MYSQL = pd.read_sql("SELECT * FROM events", con=con, index_col="index")

new_rows = len(events_no_url) - len(events_no_url_MYSQL)

## MYSQL 에 데이터를 추가한다.
if new_rows > 0:
    with engine.connect() as con:
        events_no_url.tail(new_rows).to_sql(con=con, name='events', if_exists='append', index=True)

fight_list_url = pd.DataFrame(columns=["Fighter", "event_index", "url"])
fight_list_no_url = pd.DataFrame(columns=["event_index"])
# for i in range(len(events_url)):
for i in range(415, len(events_url)):
    fight_url = events_url["url"][i]

    fight_list = pd.read_html(fight_url)[0]
    ## 경기 통계가 확정이 안 났으면 Null 이다. 이것은 데이터에서 제외 해야 한다.
    if fight_list.isnull().values.any():
        continue

    fight_list = fight_list[["Fighter"]]
    fight_list["event_index"] = i

    sauce = requests.get(fight_url, headers=headers)
    soup = BeautifulSoup(sauce.text, "lxml")

    ## 보너스 img 주소
    bonus = soup.find("div", {"class": "b-statistics__table-preview"}).find_all("img")
    bonus_fight = bonus[0].attrs["src"]
    bonus_perf = bonus[1].attrs["src"]
    bonus_sub = bonus[2].attrs["src"]
    bonus_ko = bonus[3].attrs["src"]

    ## 동일한 날짜에 열린 싸움 목록
    tbody = soup.find("tbody")

    fight_stat_url = []
    ### 각 fight url
    for tr in tbody.find_all("tr"):
        fight_stat_url.append(tr.attrs['data-link'])
    fight_list["url"] = fight_stat_url

    ## events_url 과 같은 맥락에서 역순으로 정렬한다.
    fight_list.sort_index(inplace=True, ascending=False)
    # events_url.reset_index(drop=True, inplace=True)

    fight_list_url = pd.concat([fight_list_url, fight_list], ignore_index=True)
    fight_list_no_url = pd.concat([fight_list_no_url, fight_list.drop(["url"], axis=1)], ignore_index=True)
    print(i, "done out of", len(events_url))
    sleep(1)

with open("fight_list_url.json", "w") as f:
    f.write(json.dumps(fight_list_url.to_dict()))