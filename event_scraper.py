from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine
import pymysql

## 1. 기본 스크래핑. 데이터 없음. 이벤트가 새로 열릴 때마다 해야 함
headers = {'User-agent': 'Mozilla/5.0'}
events_list = []
url_list = []

url = "http://www.ufcstats.com/statistics/events/completed?page=all"
sauce = requests.get(url, headers=headers)
soup = BeautifulSoup(sauce.text, 'lxml')
event_list= []
date_list = []
location_list = []
url_list = []
for tr in soup.find("tbody").find_all("tr", class_="b-statistics__table-row"):
    try:
        event_list.append(tr.find("a").text.strip())
        date_list.append(tr.find("span").text.strip())
        location_list.append(tr.find("td", class_="b-statistics__table-col b-statistics__table-col_style_big-top-padding").text.strip())
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

for fight_url in events_url["url"]:
    sauce = requests.get(fight_url, headers=headers)
    soup = BeautifulSoup(sauce.text, "lxml")

    ## 보너스 img 주소
    bonus = soup.find("div", {"class":"b-statistics__table-preview"}).find_all("img")
    bonus_fight = bonus[0].attrs["src"]
    bonus_perf = bonus[1].attrs["src"]
    bonus_sub = bonus[2].attrs["src"]
    bonus_ko = bonus[3].attrs["src"]

    tbody = soup.find("tbody")
    for tr in tbody.find_all("tr"):
        print(tr.attrs['data-link'])
    break