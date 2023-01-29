#!/usr/bin/env python
# coding: utf-8

# Block1
from bs4 import BeautifulSoup
import requests
import pandas as pd


# Block2
# ====================
# 編集エリア start
# ====================

# 取得したい年
from_year = 2022
to_year = 2023

# 最大開催数 = 5
max_number_of_times = 1

# 最大開催日 = 9
max_number_of_date = 1

# 開催地コード
# '06': '中山', '07': '中京', '09': '阪神', '10': '小倉'
venue_code_list = {'06': '中山', '07': '中京'}

# 1日のレース数
race_code_list = ['01', '02', '03']
# race_code_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# ====================
# 編集エリア end
# ====================


# Block3
# ネット競馬ドメイン
domain = 'https://race.netkeiba.com/race/result.html?race_id='


# Block4
# soup 作成
def create_soup(url):
    import time
    r = requests.get(url)
    time.sleep(1)
    soup = BeautifulSoup(r.content, 'html.parser')
    time.sleep(1)
    return soup


# Block5
# 日時データ取得
def get_date(soup, year):
    date_element = soup.find('dd', class_="Active")
    if date_element == None:
        print('レースが存在しません')
        return 'レースが存在しません'

    date = date_element.find('a').text
    if date.find('/') != -1 and date.find('(') == -1:
        date = str(year) + '年' + date.replace('/', '月') + '日'
    elif date.find('/') == -1 and date.find('(') != -1:
        date = str(year) + '年' + date_element.find('a').text[:date_element.find('a').text.find('(')]
    else:
        date = 'unknown'

    return date


# Block6
# レースデータ取得
def get_race_data(soup):
    # race date element 取得
    race_info_contents = soup.find('div', class_="RaceList_Item02")

    # race_data_01 取得
    race_data_01 = race_info_contents.find('div', class_="RaceData01")
    race_data_01_list = race_data_01.find_all('span')

    # race_data_02 取得
    race_data_02 = race_info_contents.find('div', class_="RaceData02")
    race_data_02_list = race_data_02.find_all('span')

    return [race_data_01_list, race_data_02_list]


# Block7
# レース詳細取得
def get_race_detail(race_data_01_list, race_data_02_list, date):
    race_detail = {}
    track = race_data_01_list[0].string.strip()[0]
    if track == "ダ":
        track = "ダート"
    distance = race_data_01_list[0].string.strip()[1:-1]
    track_condition = race_data_01_list[2].string.strip()[-1]
    venue = race_data_02_list[1].string.strip()
    race = race_data_02_list[3].string.strip()
    rank = race_data_02_list[4].string.strip().replace('クラス', '').replace('オープン', 'OP')

    race_detail['date'] = date
    race_detail['track'] = track
    race_detail['distance'] = distance
    race_detail['track_condition'] = track_condition
    race_detail['venue'] = venue
    race_detail['race'] = race
    race_detail['rank'] = rank

    return race_detail


# Block8
# 馬の親取得
def get_horse_parent(element):
    import time
    horse_info_page = requests.get(element.get("href"))
    time.sleep(1)
    horse_info_page_soup = BeautifulSoup(horse_info_page.content, 'html.parser')
    time.sleep(1)
    horse_info_contents = horse_info_page_soup.find('table', class_="blood_table")
    return horse_info_contents.find_all("tr")


# Block9
# 馬の詳細情報取得
def get_horse_detail(soup):
    horse_table_contents = soup.find('table', class_="RaceTable01 RaceCommon_Table ResultRefund Table_Show_All")
    get_horse_detail_tr = horse_table_contents.find_all("tr")
    horse_list = []
    number_of_horse = 3

    for i in range(number_of_horse + 1):
        horse_info = {}
        if i != 0:
            # 馬の名前取得
            name = get_horse_detail_tr[i].find('a')
            horse_info['order'] = str(i)
            horse_info['name'] = name.string

            # 馬の親取得
            horse_parent = get_horse_parent(name)
            horse_info['father'] = horse_parent[0].find('a').string
            horse_info['mother'] = horse_parent[2].find('a').string

            # 斤量
            horse_info['weight'] = get_horse_detail_tr[i].find('span', class_="JockeyWeight").string

            # 騎手
            jockey = get_horse_detail_tr[i].find_all('a')[1]
            if bool(jockey.find('font')):
                horse_info['jockey'] = jockey.find('font').string.strip()
            else:
                horse_info['jockey'] = jockey.string.strip()

            # タイム
            horse_info['time'] = get_horse_detail_tr[i].find('span', class_="RaceTime").string

            # 人気
            horse_info['odds'] = get_horse_detail_tr[i].find('span', class_="OddsPeople").string

            # リストに追加
            horse_list.append(horse_info)

    return horse_list


# Block10
# 払い戻しの情報取得
def get_refund_info(soup):
    refund_contents = soup.find('div', class_="FullWrap")
    refund_info = {}
    # 単勝
    tansho_element = refund_contents.find('tr', class_="Tansho")
    tansho = tansho_element.find('td', class_="Payout").string
    refund_info['tansho'] = tansho

    # 馬連
    umaren_element = refund_contents.find('tr', class_="Umaren")
    umaren = umaren_element.find('td', class_="Payout").string
    refund_info['umaren'] = umaren

    # 馬単
    umatan_element = refund_contents.find('tr', class_="Umatan")
    umatan = umatan_element.find('td', class_="Payout").string
    refund_info['umatan'] = umatan

    # 三連複
    fuku3_element = refund_contents.find('tr', class_="Fuku3")
    fuku3 = fuku3_element.find('td', class_="Payout").string
    refund_info['fuku3'] = fuku3

    # 三連単
    tan3_element = refund_contents.find('tr', class_="Tan3")
    tan3 = tan3_element.find('td', class_="Payout").string
    refund_info['tan3'] = tan3

    return refund_info


# Block11
# レースごとのデータリスト作成
result_list = []
for year in range(to_year - from_year + 1):
    target_year = from_year + year
    print('====================')
    print('Start ' + str(target_year) + ' Race')
    print('====================')

    venue_code_result_list = []
    for venue_code in venue_code_list:

        times_code_result_list = []
        for i in range(max_number_of_times):
            times_code = '0' + str(i+1)
            print('Start ' + str(target_year) + ' ' + venue_code_list[venue_code] + ' 第' + times_code + '回')

            date_code_result_list = []
            for k in range(max_number_of_date):
                date_code = '0' + str(k+1)
                print(date_code + '日目')

                race_code_result_list = []
                for race_code in race_code_list:
                    # url 作成
                    # `https://race.netkeiba.com/race/result.html?race_id=${yyyy}${venue_code}${times_code}${date_code}${race_code}`
                    url = '{}{}{}{}{}{}'.format(domain, target_year, venue_code, times_code, date_code, race_code)
                    print(url)
                    soup = create_soup(url)

                    # 日時取得
                    date = get_date(soup, target_year)
                    if date == 'レースが存在しません':
                        break

                    # レースデータ取得
                    [race_data_01_list, race_data_02_list] = get_race_data(soup)

                    # レース詳細取得
                    race_detail = get_race_detail(race_data_01_list, race_data_02_list, date)

                    # 馬の詳細情報取得
                    horse_list = get_horse_detail(soup)

                    # 払い戻しの情報取得
                    refund = get_refund_info(soup)
                    race_code_result_list.append([{'race_detail': race_detail}, {'horse_list': horse_list}, {'refund': refund}])

                date_code_result_list.append(race_code_result_list)

            print('Finished ' + str(target_year) + ' ' + venue_code_list[venue_code] + ' 第' + times_code + '回')
            print('')
            times_code_result_list.append(date_code_result_list)

        venue_code_result_list.append(times_code_result_list)

    print('====================')
    print('Finished ' + str(target_year) + ' Race')
    print('====================')
    result_list.append(venue_code_result_list)


# Block12
# 出力データ整形
date_list = []
track_list = []
distance_list = []
# TODO 牝馬限定戦
filly_limited_list = []
# TODO 重賞
grand_prize_list = []
track_condition_list = []
venue_list = []
race_list = []
rank_list = []
name1_list = []
father1_list = []
mother1_list = []
weight1_list = []
jockey1_list = []
time1_list = []
odds1_list = []
name2_list = []
father2_list = []
mother2_list = []
weight2_list = []
jockey2_list = []
time2_list = []
odds2_list = []
name3_list = []
father3_list = []
mother3_list = []
weight3_list = []
jockey3_list = []
time3_list = []
odds3_list = []
tansho_list = []
umaren_list = []
umatan_list = []
fuku3_list = []
tan3_list = []
for year in result_list:
    for vens in year:
        for times in vens:
            for dates in times:
                for race in dates:
                    date_list.append(race[0]['race_detail']['date'])
                    track_list.append(race[0]['race_detail']['track'])
                    distance_list.append(race[0]['race_detail']['distance'])
                    filly_limited_list.append('')
                    grand_prize_list.append('')
                    track_condition_list.append(race[0]['race_detail']['track_condition'])
                    venue_list.append(race[0]['race_detail']['venue'])
                    race_list.append(race[0]['race_detail']['race'])
                    rank_list.append(race[0]['race_detail']['rank'])
                    name1_list.append(race[1]['horse_list'][0]['name'])
                    father1_list.append(race[1]['horse_list'][0]['father'])
                    mother1_list.append(race[1]['horse_list'][0]['mother'])
                    weight1_list.append(race[1]['horse_list'][0]['weight'])
                    jockey1_list.append(race[1]['horse_list'][0]['jockey'])
                    time1_list.append(race[1]['horse_list'][0]['time'])
                    odds1_list.append(race[1]['horse_list'][0]['odds'])
                    name2_list.append(race[1]['horse_list'][1]['name'])
                    father2_list.append(race[1]['horse_list'][1]['father'])
                    mother2_list.append(race[1]['horse_list'][1]['mother'])
                    weight2_list.append(race[1]['horse_list'][1]['weight'])
                    jockey2_list.append(race[1]['horse_list'][1]['jockey'])
                    time2_list.append(race[1]['horse_list'][1]['time'])
                    odds2_list.append(race[1]['horse_list'][1]['odds'])
                    name3_list.append(race[1]['horse_list'][2]['name'])
                    father3_list.append(race[1]['horse_list'][2]['father'])
                    mother3_list.append(race[1]['horse_list'][2]['mother'])
                    weight3_list.append(race[1]['horse_list'][2]['weight'])
                    jockey3_list.append(race[1]['horse_list'][2]['jockey'])
                    time3_list.append(race[1]['horse_list'][2]['time'])
                    odds3_list.append(race[1]['horse_list'][2]['odds'])
                    tansho_list.append(race[2]['refund']['tansho'])
                    umaren_list.append(race[2]['refund']['umaren'])
                    umatan_list.append(race[2]['refund']['umatan'])
                    fuku3_list.append(race[2]['refund']['fuku3'])
                    tan3_list.append(race[2]['refund']['tan3'])


# Block13
# データフォーマット化
result = pd.DataFrame({'日付': date_list,'会場': track_list,'距離': distance_list,'牝馬限定戦': filly_limited_list,'馬場': venue_list,'馬場状態': track_condition_list,'レース': race_list,'条件': rank_list,'1着タイム': time1_list,'2着タイム': time2_list,'3着タイム': time3_list,'重賞': grand_prize_list,'1着馬': name1_list,'1着馬騎手': jockey1_list,'1着馬斤量': weight1_list,'1着馬人気': odds1_list,'1着馬父': father1_list,'1着馬母': mother1_list,'2着馬': name2_list,'2着馬騎手': jockey2_list,'2着馬斤量': weight2_list,'2着馬人気': odds2_list,'2着馬父': father2_list,'2着馬母': mother2_list,'3着馬': name3_list,'3着馬騎手': jockey3_list,'3着馬斤量': weight3_list,'3着馬人気': odds3_list,'3着馬父': father3_list,'3着馬母': mother3_list,'単勝': tansho_list,'馬連': umaren_list,'馬単': umatan_list,'三連複': fuku3_list,'三連単': tan3_list})


# Block14
# csv 出力
result.to_csv('keiba.csv', index=False, encoding='utf-8')
