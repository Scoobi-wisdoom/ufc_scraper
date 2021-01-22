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
events = soup.find_all(class_="b-link b-link_style_black")

for event in events:
    url_list.append(event.get_attribute_list('href')[0])
    events_list.append(event.text.strip())

data = [events_list, url_list]
events_url = pd.DataFrame({"event": events_list, "url": url_list})
events_url.sort_index(inplace=True, ascending=False)
events_url.reset_index(drop=True, inplace=True)

# 2. MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

## MYSQL 에 데이터를 처음 입력한다.
# with engine.connect() as con:
#     events_url.to_sql(con=con, name='events', if_exists='replace', index=True)
#     con.execute('ALTER TABLE `events` ADD PRIMARY KEY (`index`);')

## MYSQL DB 와 스크래핑 데이터를 비교한다.
with engine.connect() as con:
    events_url_MYSQL = pd.read_sql("SELECT * FROM events", con=con, index_col="index")

new_rows = len(events_url) - len(events_url_MYSQL)

## MYSQL 에 데이터를 추가한다.
if new_rows > 0:
    with engine.connect() as con:
        events_url.tail(new_rows).to_sql(con=con, name='events', if_exists='append', index=True)

## 보너스 여부 판단
bonus_fight = "http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/fight.png"
bonus_perf = "http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/perf.png"
bonus_sub = "http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/sub.png"
bonus_ko = "http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/ko.png"

for event_url in events_url["url"]:
    sauce = requests.get(event_url, event_url)
    soup = BeautifulSoup(sauce.text, "lxml")
    tbody = soup.find("tbody")
    for tr in tbody.find_all("tr"):
        print(tr.attrs['data-link'])
    break