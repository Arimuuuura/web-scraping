#!/usr/bin/env python
# coding: utf-8


# Block1
from concurrent.futures import ThreadPoolExecutor
import time
from bs4 import BeautifulSoup
import requests
import pandas as pd
from itertools import repeat
import datetime
import os

number_of_max_workers=None
waiting_to_load_sec = 1
time_to_unload_sec = 60
retry_count = 3
domain = 'https://race.netkeiba.com/race/result.html?race_id='


# Block2
# ====================
# 編集エリア start
# ====================

# 取得したい年
from_year = 2010
to_year = 2015

# 開催数(第○回)
# 最大 = '05'
# '01', '02', '03', '04', '05'
times_code_list = ['01']

# 開催日(○日目)
# 最大 = '09'
# '01', '02', '03', '04', '05', '06', '07', '08', '09'
date_code_list = ['01']

# 開催地コード
# '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京', '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉'
venue_code_list = {'01': '札幌', '02': '函館'}

# 1日のレース数
# 最大 = '12'
race_code_list = ['01', '02']

# csvファイルの出力先
# './Desktop'
output_location = './'

# ====================
# 編集エリア end
# ====================


# Block3
# create soup
def create_soup(url):
    for i in range(retry_count):
        try:
            r = requests.get(url)
            time.sleep(waiting_to_load_sec)
            if r.status_code == 200:
                break
            else:
                print(f'{url} Retry count: {i+1}')
                time.sleep(waiting_to_load_sec)
        except requests.exceptions.HTTPError as err:
            print(f'err: {err}')
            print(f'url: {url}')

    soup = BeautifulSoup(r.content, 'html.parser')
    time.sleep(waiting_to_load_sec)
    return soup


# Block4
# 日時取得
def get_date(soup, year):
    date_element = soup.find('dd', class_="Active")
    if date_element == None:
        return False
    date = date_element.find('a').text
    if date.find('/') != -1 and date.find('(') == -1:
        date = str(year) + '年' + date.replace('/', '月') + '日'
    elif date.find('/') == -1 and date.find('(') != -1:
        date = str(year) + '年' + date_element.find('a').text[:date_element.find('a').text.find('(')]
    else:
        date = 'unknown'
    return date


# Block5
# レースデータ取得
def get_race_data_list(soup):
    # race date element 取得
    race_info_contents = soup.find('div', class_="RaceList_Item02")
    # race_data_01 取得
    race_data_01 = race_info_contents.find('div', class_="RaceData01")
    race_data_01_list = race_data_01.find_all('span')
    # race_data_02 取得
    race_data_02 = race_info_contents.find('div', class_="RaceData02")
    race_data_02_list = race_data_02.find_all('span')
    return [race_data_01_list, race_data_02_list]


# Block6
# レース詳細取得
def get_race_detail(date, race_data_01_list, race_data_02_list):
    race_detail = {}
    race_detail['date'] = date
    track = race_data_01_list[0].string.strip()[0] if bool(race_data_01_list[0].string) else 'unknown'
    race_detail['track'] = "ダート" if track == "ダ" else track
    race_detail['distance'] = race_data_01_list[0].string.strip()[1:-1] if bool(race_data_01_list[0].string) else 'unknown'
    race_detail['track_condition'] = race_data_01_list[2].string.strip()[-1] if bool(race_data_01_list[2].string) else 'unknown'
    race_detail['venue'] = race_data_02_list[1].string.strip() if bool(race_data_02_list[1].string) else 'unknown'
    race_detail['race'] = race_data_02_list[3].string.strip() if bool(race_data_02_list[3].string) else 'unknown'
    race_detail['rank'] = race_data_02_list[4].string.strip().replace('クラス', '').replace('オープン', 'OP') if bool(race_data_02_list[4].string) else 'unknown'
    return race_detail


# Block7
# 馬の親取得
def get_horse_parent(element):
    import time
    parent_url = element.get("href")
    for i in range(retry_count):
        try:
            horse_info_page = requests.get(parent_url)
            time.sleep(waiting_to_load_sec)
            if horse_info_page.status_code == 200:
                break
            else:
                print(f'{parent_url} Retry count: {i+1}')
                time.sleep(waiting_to_load_sec)
        except requests.exceptions.HTTPError as err:
            print(f'err: {err}')
            print(f'url: {parent_url}')
    horse_info_page_soup = BeautifulSoup(horse_info_page.content, 'html.parser')
    time.sleep(waiting_to_load_sec)
    horse_info_contents = horse_info_page_soup.find('table', class_="blood_table")
    return horse_info_contents.find_all("tr")


# Block8
# 1~3着の馬情報取得
def get_horse_detail(soup):
    horse_table_contents = soup.find('table', class_="RaceTable01 RaceCommon_Table ResultRefund Table_Show_All")
    get_horse_detail_tr = horse_table_contents.find_all("tr")
    horse_list = []
    for i in [1, 2, 3]:
        horse_info = {}
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
        horse_info['jockey'] = jockey.find('font').string.strip() if bool(jockey.find('font')) else jockey.string.strip()
        # タイム
        horse_info['time'] = get_horse_detail_tr[i].find('span', class_="RaceTime").string
        # 人気
        horse_info['odds'] = get_horse_detail_tr[i].find('span', class_="OddsPeople").string
        # リストに追加
        horse_list.append(horse_info)
    return horse_list


# Block9
# 払い戻しの情報取得
def get_refund(soup):
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


# Block10
# レースデータ取得
def get_race_result(domain, target_year, venue_code, times_code, date_code, race_code):
    url = f'{domain}{target_year}{venue_code}{times_code}{date_code}{race_code}'
    # create soup
    soup = create_soup(url)
    # 日時取得
    date = get_date(soup, target_year)
    if not bool(date):
        time.sleep(waiting_to_load_sec * 3)
        return False
    # レースデータ取得
    [race_data_01_list, race_data_02_list] = get_race_data_list(soup)
    # レース詳細取得
    race_detail = get_race_detail(date, race_data_01_list, race_data_02_list)
    # 1~3着の馬情報取得
    horse_list = get_horse_detail(soup)
    # 払い戻しの情報取得
    refund_info = get_refund(soup)
    return [{'race_detail': race_detail}, {'horse_list': horse_list}, {'refund': refund_info}, {'url': url}]


# Block11
# 出力データ整形
def output_data_formatting(result_list):
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
    url_list = []
    for vens in result_list:
        for times in vens:
            for dates in times:
                for race in dates:
                    if bool(race):
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
                        url_list.append(race[3]['url'])
    return [date_list, track_list, distance_list, filly_limited_list, grand_prize_list, track_condition_list, venue_list, race_list, rank_list, name1_list, father1_list, mother1_list, weight1_list, jockey1_list, time1_list, odds1_list, name2_list, father2_list, mother2_list, weight2_list, jockey2_list, time2_list, odds2_list, name3_list, father3_list, mother3_list, weight3_list, jockey3_list, time3_list, odds3_list, tansho_list, umaren_list, umatan_list, fuku3_list, tan3_list, url_list]


# Block12
# データフォーマット化
def data_formatting(date_list, track_list, distance_list, filly_limited_list, grand_prize_list, track_condition_list, venue_list, race_list, rank_list, name1_list, father1_list, mother1_list, weight1_list, jockey1_list, time1_list, odds1_list, name2_list, father2_list, mother2_list, weight2_list, jockey2_list, time2_list, odds2_list, name3_list, father3_list, mother3_list, weight3_list, jockey3_list, time3_list, odds3_list, tansho_list, umaren_list, umatan_list, fuku3_list, tan3_list, url_list):
 return pd.DataFrame({'日付': date_list,'会場': track_list,'距離': distance_list,'牝馬限定戦': filly_limited_list,'馬場': venue_list,'馬場状態': track_condition_list,'レース': race_list,'条件': rank_list,'1着タイム': time1_list,'2着タイム': time2_list,'3着タイム': time3_list,'重賞': grand_prize_list,'1着馬': name1_list,'1着馬騎手': jockey1_list,'1着馬斤量': weight1_list,'1着馬人気': odds1_list,'1着馬父': father1_list,'1着馬母': mother1_list,'2着馬': name2_list,'2着馬騎手': jockey2_list,'2着馬斤量': weight2_list,'2着馬人気': odds2_list,'2着馬父': father2_list,'2着馬母': mother2_list,'3着馬': name3_list,'3着馬騎手': jockey3_list,'3着馬斤量': weight3_list,'3着馬人気': odds3_list,'3着馬父': father3_list,'3着馬母': mother3_list,'単勝': tansho_list,'馬連': umaren_list,'馬単': umatan_list,'三連複': fuku3_list,'三連単': tan3_list,'url': url_list})


# Block13
# csv 出力
def export_csv(result, target_year, venue_code):
    file_name = f'keiba{target_year}_{venue_code}.csv'
    if not os.path.exists(f'{output_location}/{target_year}'):
        os.mkdir(f'{output_location}/{target_year}')

    if os.path.exists(f'{output_location}/{target_year}/{file_name}'):
        [year, month, date, hour, minute, second] = get_now_time(False)
        file_name = f'keiba{target_year}_{venue_code}_{hour}{minute}{second}.csv'

    result.to_csv(f'{output_location}/{target_year}/{file_name}', index=False, encoding='utf-8_sig')
    return file_name


# Block14
# 現在時刻取得
def get_now_time(format_flg=True):
    dt_now = datetime.datetime.now()
    year = dt_now.strftime('%Y')
    month = dt_now.strftime('%m')
    date = dt_now.strftime('%d')
    hour = dt_now.strftime('%H')
    minute = dt_now.strftime('%M')
    second = dt_now.strftime('%S')
    if bool(format_flg):
        return dt_now.strftime('%Y-%m-%d %H:%M:%S')
    return [year, month, date, hour, minute, second]


# Block15
# レースごとのデータリスト作成
for year in range(to_year - from_year + 1):
    result_list = []
    target_year = from_year + year
    print('========================================')
    print(f'[{get_now_time()}] Start {target_year} Race')
    print('========================================')

    for venue_code in venue_code_list:
        venue_code_result_list = []
        print('')
        print(f'[{get_now_time()}] Starting {target_year} {venue_code_list[venue_code]} 🏇🏇🏇')
        print('')
        times_code_result_list = []

        for times_code in times_code_list:
            if times_code == '01':
                suffix = 'st'
            elif times_code == '02':
                suffix = 'nd'
            elif times_code == '03':
                suffix = 'rd'
            else:
                suffix = 'th'
            print(f'[{get_now_time()}] {venue_code_list[venue_code]} {times_code}{suffix} ===>>>')
            date_code_result_list = []

            for date_code in date_code_list:
                print(f'[{get_now_time()}] Day {date_code} Loading...')
                # レース取得の並列処理
                with ThreadPoolExecutor(max_workers=number_of_max_workers) as executor:
                    race_code_result = executor.map(get_race_result, repeat(domain), repeat(target_year), repeat(venue_code), repeat(times_code), repeat(date_code), race_code_list)
                race_code_result_list = list(race_code_result)
                date_code_result_list.append(race_code_result_list)
                time.sleep(time_to_unload_sec / 6)

            print(f'[{get_now_time()}] {venue_code_list[venue_code]} {times_code}{suffix} <<<===')
            print(f'[{get_now_time()}] Wait for {time_to_unload_sec * 3}sec.')
            time.sleep(time_to_unload_sec * 3)
            times_code_result_list.append(date_code_result_list)

        venue_code_result_list.append(times_code_result_list)
        print(f'[{get_now_time()}] Finished {target_year} {venue_code_list[venue_code]}')
        [date_list, track_list, distance_list, filly_limited_list, grand_prize_list, track_condition_list, venue_list, race_list, rank_list, name1_list, father1_list, mother1_list, weight1_list, jockey1_list, time1_list, odds1_list, name2_list, father2_list, mother2_list, weight2_list, jockey2_list, time2_list, odds2_list, name3_list, father3_list, mother3_list, weight3_list, jockey3_list, time3_list, odds3_list, tansho_list, umaren_list, umatan_list, fuku3_list, tan3_list, url_list] = output_data_formatting(venue_code_result_list)
        data_result = data_formatting(date_list, track_list, distance_list, filly_limited_list, grand_prize_list, track_condition_list, venue_list, race_list, rank_list, name1_list, father1_list, mother1_list, weight1_list, jockey1_list, time1_list, odds1_list, name2_list, father2_list, mother2_list, weight2_list, jockey2_list, time2_list, odds2_list, name3_list, father3_list, mother3_list, weight3_list, jockey3_list, time3_list, odds3_list, tansho_list, umaren_list, umatan_list, fuku3_list, tan3_list, url_list)
        file_name = export_csv(data_result, target_year, f'{venue_code_list[venue_code]}')
        print('')
        print(f'[{get_now_time()}] ◇◇◇ Successful output of csv file in {target_year} {venue_code_list[venue_code]} ◇◇◇')
        print(f'[{get_now_time()}] ◇◇◇ Output location is {output_location}/{target_year}/{file_name} ◇◇◇')
        print('')
        print(f'[{get_now_time()}] Wait for {time_to_unload_sec * 5}sec.')
        time.sleep(time_to_unload_sec * 5)

    print('========================================')
    print(f'[{get_now_time()}] Finished {target_year} Race')
    print('========================================')
    print('')
    print(f'[{get_now_time()}] Wait for {time_to_unload_sec * 10}sec.')
    print('')
    time.sleep(time_to_unload_sec * 10)

print('')
print('========================================')
print(f'[{get_now_time()}] All work done')
print('========================================')
print('')
