import sqlalchemy
from bs4 import BeautifulSoup
import requests
import pandas as pd
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

events_url = pd.DataFrame({"event_name": event_list, "date": date_list, "location": location_list, "url": url_list})
events_url["date"] = pd.to_datetime(events_url["date"])
events_url.sort_index(inplace=True, ascending=False)
events_url.reset_index(drop=True, inplace=True)

events_no_url = events_url.drop(["url"], axis=1)

locations = pd.DataFrame(events_no_url[~events_no_url['location'].duplicated(keep='first')]['location'])
locations.reset_index(drop=True, inplace=True)
locations.rename(columns={'location': 'location_name'}, inplace=True)
locations_dict = {}
for i in range(len(locations)):
    locations_dict[locations['location_name'][i]] = i

## events_no_url 에 location_id 부여
location_id = []
for location in events_no_url['location']:
    location_id.append(locations_dict[location])
events_no_url['location_id'] = location_id
## events_no_url 에서 location 열을 삭제
events_no_url = events_no_url.drop(['location'], axis=1)

# . MYSQL 에 연결
with open("db_name.txt", "r") as f:
    lines = f.readlines()
    pw = lines[0].strip()
    db = lines[1].strip()
engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw=pw, db=db))

## MYSQL 에 데이터를 처음 입력한다: Table locations, Table events
# with engine.connect() as con:
#     ## location table
#     locations.to_sql(con=con, name='locations', if_exists='replace', index=True,
#                      dtype={None: sqlalchemy.types.INT,
#                             'location_name': sqlalchemy.types.VARCHAR(length=255)})
#     con.execute('ALTER TABLE `locations` ADD PRIMARY KEY (`index`);')
#     con.execute('ALTER TABLE `locations` CHANGE `index` `location_id` INT;')
#     ## events table
#     events_no_url.to_sql(con=con, name='events', if_exists='replace', index=True,
#                          dtype={None: sqlalchemy.types.INTEGER(),
#                                 'event_name': sqlalchemy.types.VARCHAR(length=255),
#                                 'date': sqlalchemy.types.Date(),
#                                 'location_id': sqlalchemy.types.INT})
#     con.execute('ALTER TABLE `events` ADD PRIMARY KEY (`index`);')
#     con.execute('ALTER TABLE `events` CHANGE `index` `event_id` INT;')
#     con.execute('ALTER TABLE `events` ADD FOREIGN KEY (`location_id`) REFERENCES `locations`(`location_id`);')

## MYSQL DB 와 스크래핑 데이터를 비교한다.
with engine.connect() as con:
    events_no_url_MYSQL = pd.read_sql("SELECT * FROM events", con=con, index_col="event_id")

new_rows = len(events_no_url) - len(events_no_url_MYSQL)

## MYSQL 에 데이터를 추가한다.
if new_rows > 0:
    with engine.connect() as con:
        events_no_url.tail(new_rows).to_sql(con=con, name='events', if_exists='append', index=True)

fight_list_url = pd.DataFrame(columns=["Fighter", "event_id", "url"])
fight_list_no_url = pd.DataFrame(columns=["event_id"])

for i in range(len(events_url)):
    fight_url = events_url["url"][i]

    fight_list = pd.read_html(fight_url)[0]
    ## 경기 통계가 확정이 안 났으면 Null 이다. 이것은 데이터에서 제외 해야 한다.
    if fight_list.isnull().values.any():
        continue

    # fight_list = fight_list[["Fighter"]]
    fight_list["event_id"] = i

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

# weight class 종류
weight_classes = pd.DataFrame(fight_list_url[~fight_list_url['Weight class'].duplicated(keep='first')]['Weight class'])
weight_classes.reset_index(drop=True, inplace=True)
## MYSQL 에 데이터를 처음 입력한다: Table weights
# with engine.connect() as con:
#     ## weights table
#     weight_classes.to_sql(con=con, name='weights', if_exists='replace', index=True,
#                      dtype={None: sqlalchemy.types.INT,
#                             'weight_name': sqlalchemy.types.VARCHAR(length=255)})
#     con.execute('ALTER TABLE `weights` ADD PRIMARY KEY (`index`);')
#     con.execute('ALTER TABLE `weights` CHANGE `index` `weight_id` INT;')

# 데이터 저장
# with open('fight_list_url.json', 'w') as f:
#     f.write(json.dumps(fight_list_url.to_dict()))