# -*- coding: utf-8 -*-
# import os
import scrapy
import pdb
import datetime, time
import re
import json
from lxml import etree
import requests
import pymysql.cursors
from scrapy_splash import SplashRequest
from auto_odds_compare.spiders.tools import MyTools

debugging = True
info_days = 10  # 收集多少天的信息  debugging为true时有效
payback_rate = 0.94     # 设定返还率，用来计算整体赔率
# threshold_value = 0.04  # 设定阈值
# limit_max_probability = 1    # 超过该概率的比赛不入选
limit_over_threshold_num = 0    # 设定超过阈值达到不可能方向的数目
# high_accurate_company_name_list = ["Sportsbet.com.au", "Intralot.it", "Betfair", "Sbobet", "BetVictor", "PlanetWin365",
#                                    "Betshop", "5 Dimes", "10BET", "Pinnacle", "Bet3000", "Lottery Official",
#                                    "marsbetting", "Smarkets", "Bet 365", "G England Johns", "StarPrice", "Betflag.it",
#                                    "Efbet", "Vcbet", "Sisal.it", "Bethard", "Stan James", "BetClic.it", "Marathon",
#                                    "Ladbrokes", "Betano.ro", "Gamebookers", "William Hill", "CoolBet", "Bet-at-home",
#                                    "Fonbet", "Sportsbet", "iFortuna.cz", "BETCRIS", "Betonline", "Wewbet", "Sisal",
#                                    "Goalbet", "Bwin", "Hooball", "188bet", "Coral", "iddaa", "Sweden", "Stoiximan",
#                                    "Norway", "SNAI.it", "Unibet.fr", "Expekt", "Eurobet", "babibet", "CashPoint",
#                                    "leaderbet", "Balkan Bet"]
high_accurate_company_name_list = ["Lottery Official", "Ladbrokes", "Manbetx", "Macauslot", "Vcbet"]

current_hour = time.localtime()[3]  # 获取当前的小时数，如果小于8则应该选择yesterday
nowadays = datetime.datetime.now().strftime("%Y-%m-%d")  # 获取当前日期 格式2018-01-01
yesterdy = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")  # 获取昨天日期

search_date = []
if not debugging:
    if current_hour < 8:    # 默认在早上八点前拉取昨天页面
        search_date.append(yesterdy)
    else:
        search_date.append(nowadays)
else:
    for i in range(info_days):
        if datetime.datetime.now().hour < 8:
            add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 2))).strftime("%Y-%m-%d")
        else:
            add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 1))).strftime("%Y-%m-%d")
        search_date.append(add_day)

# 比赛列表 item
class match_list_Item(scrapy.Item):
    league_name = scrapy.Field()  # 联赛名称
    home_name = scrapy.Field()  # 主队名称
    away_name = scrapy.Field()  # 客队名称
    start_time = scrapy.Field()  # 开赛时间
    match_result = scrapy.Field()  # 比赛结果(310)
    max_accurate_company = scrapy.Field()  # 最准确的公司
    min_accurate_company = scrapy.Field()  # 最不准确的公司
    support_list_text = scrapy.Field()  # 支持方向文字，例如：1_0_1
    current_rate = scrapy.Field()  # 算法评分
    current_search_date = scrapy.Field()  # 当前查询日期 用来建表

class OddSpider(scrapy.Spider):
    name = 'auto_odds_compare'
    allowed_domains = ['http://info.livescore123.com/', 'http://www.livescore123.com/']
    # download_delay = 2
    # 包装url
    start_urls = []
    for single_search_date in search_date:
        if debugging:
            url = 'http://info.livescore123.com/1x2/companyhistory.aspx?id=177&matchdate=' + single_search_date
        else:
            url = 'http://info.livescore123.com/1x2/index.htm'
        start_urls.append(url)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url)

    # 分析当天所有比赛信息
    def parse(self, response):
        need_step = False   # 标志是否要跳过
        for tr in response.xpath('//table[contains(@class,"schedule")]').xpath('tr'):
            # 如果没有tr_id说明是头部或者需要跳过则 跳过
            if len(tr.xpath('@id')) == 0 or need_step:
                need_step = False
                continue
            tr_id = tr.xpath('@id').extract()[0]
            # 如果下面成立说明是最新赔率行
            if tr_id.split('_')[0].split('tr')[-1] != '':
                single_match_tr_index = 2
            else:
                single_match_tr_index = 1
            # 如果是本场比赛首行，则去最后一个td拿链接跳转到全赔率页面
            if single_match_tr_index == 1:
                all_odds_href = tr.xpath('td')[-1].xpath('a/@href').extract()[0]    # 跳转到单场比赛全赔率页面的链接
                try:
                    if len(tr.xpath('td')[0].xpath('a')) != 0:
                        league_name = tr.xpath('td')[0].xpath('a/text()').extract()[0]    # 联赛名称(可能在a中)，是英文，需要用字典转为中文
                    else:
                        league_name = tr.xpath('td')[0].xpath('text()').extract()[0]
                except:
                    print('league_name ERROR!')
                    pdb.set_trace()
                home_name = tr.xpath('td')[2].xpath('a/text()').extract()[0]
                away_name = tr.xpath('td')[10].xpath('a/text()').extract()[0]
                start_time_year = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')','').split(',')[0])
                start_time_month = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')','').split(',')[1].split('-')[0])
                start_time_day = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')','').split(',')[2])
                start_time_hour = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')','').split(',')[3])
                start_time_minu = int(tr.xpath('td')[1].xpath('script/text()').extract()[0].replace('showtime(', '').replace(')','').split(',')[4])
                start_time = datetime.datetime(start_time_year, start_time_month, start_time_day, start_time_hour, start_time_minu) + datetime.timedelta(hours=8)
                start_mktime = time.mktime(start_time.timetuple())
                now_mktime = time.time()
                # 如果当前时间比开始时间小-3600s,则结束遍历，不再往下查找
                if (now_mktime-start_mktime) < -3600:
                    continue
                if debugging:
                    home_goal = int(tr.xpath('td')[-2].xpath('font/text()').extract()[0].split('-')[0])
                    away_goal = int(tr.xpath('td')[-2].xpath('font/text()').extract()[0].split('-')[-1])
                single_meta = {}
                single_meta['league_name'] = league_name
                single_meta['home_name'] = home_name
                single_meta['away_name'] = away_name
                single_meta['start_time'] = start_time
                if debugging:
                    single_meta['current_search_date'] = response.url.split('=')[-1]
                else:
                    single_meta['current_search_date'] = search_date[0]
                if debugging:
                    single_meta['home_goal'] = home_goal
                    single_meta['away_goal'] = away_goal
                yield SplashRequest(all_odds_href, self.all_odds_parse, meta=single_meta, args={'wait': '0.5', 'images': 0}, dont_filter=True)

    # 分析单场比赛所有赔率信息
    def all_odds_parse(self, response):
        league_name = response.meta['league_name']
        home_name = response.meta['home_name']
        away_name = response.meta['away_name']
        start_time = response.meta['start_time']
        current_search_date = response.meta['current_search_date']
        if debugging:
            home_goal = response.meta['home_goal']
            away_goal = response.meta['away_goal']

        pdb.set_trace()
        odd_match_list = []
        need_step = False  # 标志是否要跳过
        for tr in response.xpath('//table')[0].xpath('tbody/tr'):
            # 如果没有tr_id说明是头部或者需要跳过则 跳过
            if tr.xpath('@class').extract()[0] == 'ivsiinfo_td' or need_step:
                need_step = False
                continue
            td_len = len(tr.xpath('td'))    # 如果是该公司的初赔行，td为14个，当前赔率行为7个
            # 如果下面成立说明是最新赔率行
            if td_len == 14:
                single_match_tr_index = 1
            elif td_len == 7:
                single_match_tr_index = 2
            else:
                print('td_len出错')
                single_match_tr_index = 0
                pdb.set_trace()
            # 如果是初赔行，则拿取公司名称和最后更新时间
            if single_match_tr_index == 1:
                if len(tr.xpath('td')[0].xpath('text()').extract()) == 0:
                    need_step = True
                    continue
                company_name = tr.xpath('td')[2].xpath('a/text()').extract()[0]  # 公司名称
                home_original_probability = tr.xpath('td')[6].xpath('text()').extract()[0]
                draw_original_probability = tr.xpath('td')[7].xpath('text()').extract()[0]
                away_original_probability = tr.xpath('td')[8].xpath('text()').extract()[0]
                if len(tr.xpath('td')[-1].xpath('font')) != 0:
                    update_time_text = tr.xpath('td')[-1].xpath('font/text()').extract()[0]  # 赔率最近更新时间（utc）
                else:
                    update_time_text = tr.xpath('td')[-1].xpath('text()').extract()[0]  # 赔率最近更新时间（utc）
                original_update_mktime = time.mktime(time.strptime(update_time_text, '%m-%d-%Y %H:%M'))    # 转换为时间戳
                # 因为update_time是utc时间，所以加八个小时，即28800s
                original_update_mktime += 28800
                update_timestruct = time.localtime(original_update_mktime)
                start_mktime = time.mktime(start_time.timetuple())
                # 如果最近时间比开赛时间大3600s（1小时）,则跳过该家公司，并且tr要跳两个
                if (start_mktime - original_update_mktime) > 3600:
                    need_step = True
                    continue
                update_time_text = time.strftime('%Y-%m-%d %H:%M', update_timestruct)
                single_company_dict = {
                    'company_name': company_name,
                    'update_time_text': update_time_text,
                    'home_original_probability': round(float(home_original_probability.replace('%', ''))/100, 3),
                    'draw_original_probability': round(float(draw_original_probability.replace('%', ''))/100, 3),
                    'away_original_probability': round(float(away_original_probability.replace('%', ''))/100, 3),
                }
                odd_match_list.append(single_company_dict)
            else:
                match_index = len(odd_match_list) - 1
                if len(tr.xpath('td')[0].xpath('text()').extract()) != 0:
                    try:
                        home_now_probability = tr.xpath('td')[3].xpath('text()').extract()[0]
                        draw_now_probability = tr.xpath('td')[4].xpath('text()').extract()[0]
                        away_now_probability = tr.xpath('td')[5].xpath('text()').extract()[0]
                    except:
                        print('home_name:', odd_match_list[-1]['home_name'])
                        pdb.set_trace()

                    odd_match_list[match_index]['home_now_probability'] = round(
                        float(home_now_probability.replace('%', '')) / 100, 3)
                    odd_match_list[match_index]['draw_now_probability'] = round(
                        float(draw_now_probability.replace('%', '')) / 100, 3)
                    odd_match_list[match_index]['away_now_probability'] = round(
                        float(away_now_probability.replace('%', '')) / 100, 3)
                else:
                    # 如果最新赔率行没有信息，就使用原始赔率
                    home_now_probability = odd_match_list[match_index]['home_original_probability']
                    draw_now_probability = odd_match_list[match_index]['draw_original_probability']
                    away_now_probability = odd_match_list[match_index]['away_original_probability']
                    odd_match_list[match_index]['home_now_probability'] = home_now_probability
                    odd_match_list[match_index]['draw_now_probability'] = draw_now_probability
                    odd_match_list[match_index]['away_now_probability'] = away_now_probability

        if len(odd_match_list) == 0:
            return False
        # 计算赛果
        match_result = ''
        if debugging:
            if home_goal > away_goal:
                match_result = '3'
            elif home_goal == away_goal:
                match_result = '1'
            else:
                match_result = '0'

        # 计算平均概率
        average_home_now_probability = MyTools.list_average([item['home_now_probability'] for item in odd_match_list])
        max_home_now_probability = max([item['home_now_probability'] for item in odd_match_list])
        max_home_now_probability_index = [item['home_now_probability'] for item in odd_match_list].index(max_home_now_probability)
        max_home_now_probability_company = odd_match_list[max_home_now_probability_index]['company_name']
        min_home_now_probability = min([item['home_now_probability'] for item in odd_match_list])
        min_home_now_probability_index = [item['home_now_probability'] for item in odd_match_list].index(min_home_now_probability)
        min_home_now_probability_company = odd_match_list[min_home_now_probability_index]['company_name']
        # 求((max-a) - (a-min))
        max_a_min_home_probability_differ = round((max_home_now_probability + min_home_now_probability) - 2 * average_home_now_probability,2)


        average_draw_now_probability = MyTools.list_average([item['draw_now_probability'] for item in odd_match_list])
        max_draw_now_probability = max([item['draw_now_probability'] for item in odd_match_list])
        max_draw_now_probability_index = [item['draw_now_probability'] for item in odd_match_list].index(max_draw_now_probability)
        max_draw_now_probability_company = odd_match_list[max_draw_now_probability_index]['company_name']
        min_draw_now_probability = min([item['draw_now_probability'] for item in odd_match_list])
        min_draw_now_probability_index = [item['draw_now_probability'] for item in odd_match_list].index(min_draw_now_probability)
        min_draw_now_probability_company = odd_match_list[min_draw_now_probability_index]['company_name']
        # 求((max-a) - (a-min))
        max_a_min_draw_probability_differ = round((max_draw_now_probability + min_draw_now_probability) - 2 * average_draw_now_probability,2)

        average_away_now_probability = MyTools.list_average([item['away_now_probability'] for item in odd_match_list])
        max_away_now_probability = max([item['away_now_probability'] for item in odd_match_list])
        max_away_now_probability_index = [item['away_now_probability'] for item in odd_match_list].index(max_away_now_probability)
        max_away_now_probability_company = odd_match_list[max_away_now_probability_index]['company_name']
        min_away_now_probability = min([item['away_now_probability'] for item in odd_match_list])
        min_away_now_probability_index = [item['away_now_probability'] for item in odd_match_list].index(min_away_now_probability)
        min_away_now_probability_company = odd_match_list[min_away_now_probability_index]['company_name']
        # 求((max-a) - (a-min))
        max_a_min_away_probability_differ = round((max_away_now_probability + min_away_now_probability) - 2 * average_away_now_probability,2)

        # 找出最准确公司和最不准确公司
        max_accurate_company = ''
        min_accurate_company = ''
        if debugging:
            if match_result == '3':
                max_accurate_company = max_home_now_probability_company
                min_accurate_company = min_home_now_probability_company
            elif match_result == '1':
                max_accurate_company = max_draw_now_probability_company
                min_accurate_company = min_draw_now_probability_company
            else:
                max_accurate_company = max_away_now_probability_company
                min_accurate_company = min_away_now_probability_company

        # 如果是低赔率则跳过
        # if average_home_now_probability >= limit_max_probability or average_away_now_probability >= limit_max_probability:
        #     return False

        # 计算出当前赔率下，单个公司比平均概率小且超过设定阈值的数目，达到一定数目判定该方向不可能
        # home_over_threshold_num = MyTools.over_threshold_num([item['home_now_probability'] for item in odd_match_list], average_home_now_probability, threshold_value, -1)
        # draw_over_threshold_num = MyTools.over_threshold_num([item['draw_now_probability'] for item in odd_match_list], average_draw_now_probability, threshold_value, -1)
        # away_over_threshold_num = MyTools.over_threshold_num([item['away_now_probability'] for item in odd_match_list], average_away_now_probability, threshold_value, -1)

        average_home_odd = round(payback_rate/average_home_now_probability, 2)
        average_draw_odd = round(payback_rate/average_draw_now_probability, 2)
        average_away_odd = round(payback_rate/average_away_now_probability, 2)

        support_list = ['0', '0', '0']   # 长度为3，10分别表示胜平负是否支持
        # 如果最大概率公司不同，则可能有支持方向
        if max_home_now_probability_company in high_accurate_company_name_list:
            support_list[0] = '1'
        if max_draw_now_probability_company in high_accurate_company_name_list:
            support_list[1] = '1'
        if max_away_now_probability_company in high_accurate_company_name_list:
            support_list[2] = '1'
        support_list_text = support_list[0] + '_' + support_list[1] + '_' + support_list[2]

        current_rate = 0    # 当前算法评分
        if debugging and (support_list[0] != support_list[1] or support_list[0] != support_list[2]):
            if int(support_list[0]) + int(support_list[1]) + int(support_list[2]) == 1:
                if support_list[0] == '1':
                    if match_result == '3':
                        current_rate = average_home_odd - 1
                    else:
                        current_rate = -1
                elif support_list[1] == '1':
                    if match_result == '1':
                        current_rate = average_draw_odd - 1
                    else:
                        current_rate = -1
                else:
                    if match_result == '0':
                        current_rate = average_away_odd - 1
                    else:
                        current_rate = -1
            else:
                if support_list[0] == '1' and support_list[1] == '1':
                    if match_result == '3' or match_result == '1':
                        current_rate = round((average_home_odd + average_draw_odd)/4, 2) - 1
                    else:
                        current_rate = -1
                if support_list[0] == '1' and support_list[2] == '1':
                    if match_result == '3' or match_result == '0':
                        current_rate = round((average_home_odd + average_away_odd)/4, 2) - 1
                    else:
                        current_rate = -1
                if support_list[1] == '1' and support_list[2] == '1':
                    if match_result == '1' or match_result == '0':
                        current_rate = round((average_draw_odd + average_away_odd)/4, 2) - 1
                    else:
                        current_rate = -1

        single_match_Item = match_list_Item()
        single_match_Item['league_name'] = league_name
        single_match_Item['home_name'] = home_name
        single_match_Item['away_name'] = away_name
        start_time_text = start_time.strftime('%Y-%m-%d %H:%M')
        single_match_Item['start_time'] = start_time_text
        single_match_Item['match_result'] = match_result
        single_match_Item['max_accurate_company'] = max_accurate_company
        single_match_Item['min_accurate_company'] = min_accurate_company
        single_match_Item['support_list_text'] = support_list_text
        single_match_Item['current_rate'] = current_rate
        single_match_Item['current_search_date'] = current_search_date

        yield single_match_Item









