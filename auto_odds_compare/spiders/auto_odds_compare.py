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
from auto_odds_compare.items import OddSpiderItem
from scrapy_splash import SplashRequest
from auto_odds_compare.spiders.tools import MyTools
from scrapy_redis.spiders import RedisSpider

debugging = True
info_days = 356  # 收集多少天的信息  debugging为true时有效
need_company_id = '156'     # pinnacle

# high_accurate_company_name_list = ["Sportsbet.com.au", "Intralot.it", "Betfair", "Sbobet", "BetVictor", "PlanetWin365",
#                                    "Betshop", "5 Dimes", "10BET", "Pinnacle", "Bet3000", "Lottery Official",
#                                    "marsbetting", "Smarkets", "Bet 365", "G England Johns", "StarPrice", "Betflag.it",
#                                    "Efbet", "Vcbet", "Sisal.it", "Bethard", "Stan James", "BetClic.it", "Marathon",
#                                    "Ladbrokes", "Betano.ro", "Gamebookers", "William Hill", "CoolBet", "Bet-at-home",
#                                    "Fonbet", "Sportsbet", "iFortuna.cz", "BETCRIS", "Betonline", "Wewbet", "Sisal",
#                                    "Goalbet", "Bwin", "Hooball", "188bet", "Coral", "iddaa", "Sweden", "Stoiximan",
#                                    "Norway", "SNAI.it", "Unibet.fr", "Expekt", "Eurobet", "babibet", "CashPoint",
#                                    "leaderbet", "Balkan Bet"]
# high_accurate_company_name_list = ["Lottery Official", "Ladbrokes", "Manbetx", "Macauslot", "Vcbet"]

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
        if not debugging:
            if datetime.datetime.now().hour < 8:
                add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 2))).strftime("%Y-%m-%d")
            else:
                add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 1))).strftime("%Y-%m-%d")
            search_date.append(add_day)
        else:
            add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 2))).strftime("%Y-%m-%d")
            search_date.append(add_day)

class OddSpider(scrapy.Spider):
# class OddSpider(RedisSpider):
    name = 'auto_odds_compare'
    allowed_domains = ['http://1x2.7m.hk/']
    download_delay = 2
    # 包装url
    start_urls = []
    for single_search_date in search_date:
        if debugging:
            url = 'http://1x2.7m.hk/result_en.shtml?dt=' + single_search_date + '&cid='
        else:
            url = 'http://info.livescore123.com/1x2/index.htm'
        start_urls.append(url)
    # redis_key = 'OddSpider:start_urls'

    # global splashurl
    # splashurl = "http://192.168.99.100:8050/render.html";
    # 此处是重父类方法，并使把url传给splash解析
    def make_requests_from_url(self, url):
        global splashurl;
        url = splashurl + "?url=" + url;
        # 使用代理访问
        proxy = MyTools.get_proxy()
        LUA_SCRIPT = """
                    function main(splash)
                        splash:on_request(function(request)
                            request:set_proxy{
                                host = "%(host)s",
                                port = %(port)s,
                                username = '', password = '', type = "HTTPS",
                            }
                        end)
                        assert(splash:go(args.url))
                        assert(splash:wait(0.5))
                        return {
                            html = splash:html(),
                        }
                    end
                    """
        proxy_host = proxy.strip().split(':')[0]
        proxy_port = int(proxy.strip().split(':')[-1])
        LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
        try:
            print('line95,当前代理为：', "http://{}".format(proxy))
            return SplashRequest(url, self.parse,
                                args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT},
                                dont_filter=True)
        except Exception as err:
            MyTools.delete_proxy(proxy)

    def start_requests(self):
        for url in self.start_urls:
            # 使用代理访问
            proxy = MyTools.get_proxy()

            LUA_SCRIPT = """
                        function main(splash)
                            splash:on_request(function(request)
                                request:set_proxy{
                                    host = "%(host)s",
                                    port = %(port)s,
                                    username = '', password = '', type = "HTTPS",
                                }
                            end)
                            assert(splash:go(args.url))
                            assert(splash:wait(0.5))
                            return {
                                html = splash:html(),
                            }
                        end
                        """
            proxy_host = proxy.strip().split(':')[0]
            proxy_port = int(proxy.strip().split(':')[-1])
            LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
            try:
                print('line70,当前代理为：', "http://{}".format(proxy))
                yield SplashRequest(url, self.parse, args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT}, dont_filter=True)
            except Exception as err:
                MyTools.delete_proxy(proxy)

    '''
        redis中存储的为set类型的公司名称，使用SplashRequest去请求网页。
        注意：不能在make_request_from_data方法中直接使用SplashRequest（其他第三方的也不支持）,会导致方法无法执行，也不抛出异常
        但是同时重写make_request_from_data和make_requests_from_url方法则可以执行
    '''

    # def make_request_from_data(self, data):
    #     '''
    #     :params data bytes, Message from redis
    #     '''
    #     company = bytes_to_str(data, self.redis_encoding)
    #     url = self.url + '/company/basic.jspx?company=' + company
    #     return self.make_requests_from_url(url)

    # def make_requests_from_url(self, url):
    #     retry_count = 2
    #     proxy = MyTools.get_proxy()
    #     while retry_count > 0:
    #         try:
    #             # 使用代理访问
    #             print('line98,当前代理为：', "http://{}".format(proxy))
    #             yield SplashRequest(url, callback=self.parse, args={'wait': 0.5, 'images': 0, 'timeout': 30,
    #                                                        'proxy': "http://{}".format(proxy)}})
    #         except Exception:
    #             retry_count -= 1
    #     # 出错5次, 删除代理池中代理
    #     MyTools.delete_proxy(proxy)

    # 分析当天所有比赛信息
    def parse(self, response):
        print('开始分析当天所有比赛信息')
        need_step = False   # 标志是否要跳过
        for tr in response.xpath('//div[@id="odds_tb"]/table/tbody/tr'):
            try:
                tr_class = tr.xpath('@class').extract()[0]  # 如果前三个字符不是dtd说明不是比赛line，要跳过
            except Exception as e:
                # 说明不存在class，这时也要跳过
                need_step = False
                continue
            if tr_class[:3] != 'dtd' or need_step:
                need_step = False
                continue

            td_len = len(tr.xpath('td'))  # 如果是该公司的初赔行，td为9个，当前赔率行为3个
            # 如果下面成立说明是最新赔率行
            if td_len == 9:
                single_match_tr_index = 1
            elif td_len == 3:
                single_match_tr_index = 2
            else:
                print('td_len出错')
                # pdb.set_trace()
                continue
            # 如果是本场比赛首行，则去最后一个td拿链接跳转到全赔率页面
            if single_match_tr_index == 1:
                match_id = tr.xpath('td')[-1].xpath('a/@href').extract()[0].split('(')[-1].split(')')[0]  # 该场比赛ID
                all_odds_href = 'http://1x2.7m.hk/list_en.shtml?id=' + match_id  # 跳转到单场比赛全赔率页面的链接
                try:
                    if len(tr.xpath('td')[0].xpath('a')) != 0:
                        league_name = tr.xpath('td')[0].xpath('a/text()').extract()[0]    # 联赛名称(可能在a中)，是英文，需要用字典转为中文
                    else:
                        league_name = tr.xpath('td')[0].xpath('text()').extract()[0]
                except:
                    print('league_name ERROR!')
                    # pdb.set_trace()
                home_name = tr.xpath('td')[2].xpath('text()').extract()[0]
                away_name = tr.xpath('td')[6].xpath('text()').extract()[0]
                start_time_year = int(response.url.split('=')[-2].split('-')[0])
                start_time_month = int(response.url.split('=')[-2].split('-')[1])
                start_time_day = int(response.url.split('=')[-2].split('-')[2].split('&')[0])
                start_time_hour = int(tr.xpath('td')[1].xpath('text()').extract()[0].split(':')[0])
                start_time_minu = int(tr.xpath('td')[1].xpath('text()').extract()[0].split(':')[1])
                start_time = datetime.datetime(start_time_year, start_time_month, start_time_day, start_time_hour, start_time_minu)
                start_mktime = time.mktime(start_time.timetuple())
                # now_mktime = time.time()
                # 如果当前时间比开始时间小-3600s,则结束遍历，不再往下查找
                # if (now_mktime-start_mktime) < -3600:
                #     continue
                if debugging:
                    try:
                        half_home_goal = int(tr.xpath('td')[-2].xpath('text()').extract()[0].split('-')[0].split('(')[-1])
                        half_away_goal = int(tr.xpath('td')[-2].xpath('text()').extract()[0].split('-')[-1].split(')')[0])
                        home_goal = int(tr.xpath('td')[-2].xpath('b/text()').extract()[0].split('-')[0])
                        away_goal = int(tr.xpath('td')[-2].xpath('b/text()').extract()[0].split('-')[-1])
                    except:
                        # 有些比赛没有半场或全场比分，就跳过
                        print('获取比分出错，放弃该场比赛')
                        continue
                single_meta = {}
                single_meta['league_name'] = league_name
                single_meta['home_name'] = home_name
                single_meta['away_name'] = away_name
                single_meta['start_mktime'] = start_mktime
                if debugging:
                    single_meta['current_search_date'] = response.url.split('=')[-2].split('&')[0].replace('-', '_')
                else:
                    single_meta['current_search_date'] = search_date[0]
                if debugging:
                    single_meta['home_goal'] = home_goal
                    single_meta['away_goal'] = away_goal
                    single_meta['half_home_goal'] = half_home_goal
                    single_meta['half_away_goal'] = half_away_goal

                # 使用代理访问
                proxy = MyTools.get_proxy()
                LUA_SCRIPT = """
                            function main(splash)
                                splash:on_request(function(request)
                                    request:set_proxy{
                                        host = "%(host)s",
                                        port = %(port)s,
                                        username = '', password = '', type = "HTTPS",
                                    }
                	            end)
                                assert(splash:go(args.url))
                                assert(splash:wait(0.5))
                                return {
                                    html = splash:html(),
                                }
                            end
                            """
                proxy_host = proxy.strip().split(':')[0]
                proxy_port = int(proxy.strip().split(':')[-1])
                LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
                try:
                    print('line257,当前代理为：', "http://{}".format(proxy))
                    yield SplashRequest(all_odds_href, self.all_odds_parse, meta=single_meta,
                                        args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT},
                                        dont_filter=True)
                except Exception as err:
                    MyTools.delete_proxy(proxy)

    # 分析单场比赛所有赔率信息
    def all_odds_parse(self, response):
        league_name = response.meta['league_name']
        match_id = response.url.split('=')[-1]
        home_name = response.meta['home_name']
        away_name = response.meta['away_name']
        start_mktime = response.meta['start_mktime']
        current_search_date = response.meta['current_search_date']
        print('开始解析单场比赛', home_name)
        if debugging:
            home_goal = response.meta['home_goal']
            away_goal = response.meta['away_goal']
            half_home_goal = response.meta['half_home_goal']
            half_away_goal = response.meta['half_away_goal']

        need_step = False  # 标志是否要跳过
        # pdb.set_trace()
        current_company_id_list = []
        original_current_company_id_list = [item.xpath('td')[0].xpath('input/@value').extract() for item in response.xpath('//div[@id="odds_tb"]/table/tbody/tr')]
        for item in original_current_company_id_list:
            if item != []:
                current_company_id_list.append(item[0])
        if not need_company_id in current_company_id_list:
            return False
        if len(response.xpath('//div[@id="odds_tb"]/table/tbody/tr')) < 71:
        # 如果当前比赛小于35家公司开盘就跳过
            print('小于35家公司开盘，跳过当前比赛', home_name)
            return False
        for tr in response.xpath('//div[@id="odds_tb"]/table/tbody/tr'):
            try:
                tr_class = tr.xpath('@class').extract()[0]  # 如果前三个字符不是ltd说明不是比赛line，要跳过
            except Exception as e:
                # 说明不存在class，这时也要跳过
                need_step = False
                continue
            if tr_class[:3] != 'ltd' or need_step:
                need_step = False
                continue

            td_len = len(tr.xpath('td'))    # 如果是该公司的初赔行，td为14个，当前赔率行为7个
            # 如果下面成立说明是最新赔率行
            if td_len == 9:
                single_match_tr_index = 1
            elif td_len == 7:
                single_match_tr_index = 2
            else:
                print('td_len出错')
                single_match_tr_index = 0
                # pdb.set_trace()
            # 如果是初赔行，则拿取公司名称和最后更新时间
            if single_match_tr_index == 1:
                company_id = tr.xpath('td')[0].xpath('input/@value').extract()[0]  # 公司ID
                company_name = tr.xpath('td')[1].xpath('a/text()').extract()[0]  # 公司名称
                all_odds_href = 'http://1x2.7m.hk/log_en.shtml?id=' + match_id + '&cid=' + company_id  # 跳转到单场比赛全赔率页面的链接
                single_meta = {}
                single_meta['match_id'] = match_id
                single_meta['company_id'] = company_id
                single_meta['league_name'] = league_name
                single_meta['home_name'] = home_name
                single_meta['away_name'] = away_name
                single_meta['start_mktime'] = start_mktime
                single_meta['current_search_date'] = current_search_date
                single_meta['company_name'] = company_name
                if debugging:
                    single_meta['home_goal'] = home_goal
                    single_meta['away_goal'] = away_goal
                    single_meta['half_home_goal'] = half_home_goal
                    single_meta['half_away_goal'] = half_away_goal

                # 使用代理访问
                proxy = MyTools.get_proxy()
                LUA_SCRIPT = """
                            function main(splash)
                                splash:on_request(function(request)
                                    request:set_proxy{
                                        host = "%(host)s",
                                        port = %(port)s,
                                        username = '', password = '', type = "HTTPS",
                                    }
                                end)
                                assert(splash:go(args.url))
                                assert(splash:wait(0.5))
                                return {
                                    html = splash:html(),
                                }
                            end
                            """
                proxy_host = proxy.strip().split(':')[0]
                proxy_port = int(proxy.strip().split(':')[-1])
                LUA_SCRIPT = LUA_SCRIPT % {'host': proxy_host, 'port': proxy_port}
                try:
                    print('line343,当前代理为：', "http://{}".format(proxy))
                    yield SplashRequest(all_odds_href, self.single_company_odds_parse, meta=single_meta,
                                        args={'wait': 0.5, 'images': 0, 'timeout': 30, 'lua_source': LUA_SCRIPT},
                                        dont_filter=True)
                except Exception as err:
                    MyTools.delete_proxy(proxy)

    # 获取单家公司的所有赔率
    def single_company_odds_parse(self, response):
        league_name = response.meta['league_name']
        match_id = response.meta['match_id']
        company_id = response.meta['company_id']
        company_name = response.meta['company_name']
        home_name = response.meta['home_name']
        away_name = response.meta['away_name']
        start_mktime = response.meta['start_mktime']
        current_search_date = response.meta['current_search_date']
        print('开始解析单个公司赔率：', company_name)
        if debugging:
            home_goal = response.meta['home_goal']
            away_goal = response.meta['away_goal']
            half_home_goal = response.meta['half_home_goal']
            half_away_goal = response.meta['half_away_goal']

        if half_home_goal == half_away_goal:
            half_match_result = 1
        else:
            if half_home_goal > half_away_goal:
                half_match_result = 3
            else:
                half_match_result = 0

        if home_goal == away_goal:
            match_result = 1
        else:
            if home_goal > away_goal:
                match_result = 3
            else:
                match_result = 0

        need_step = False  # 标志是否要跳过
        count_index = 0     # 遍历赔率行时计数，方便数据库按顺序排序
        for tr in response.xpath('//div[@id="log_tb"]/table/tbody/tr'):
            # 如果tr有class存在，说明是头部需要跳过
            if len(tr.xpath('@class').extract()) > 0 or need_step:
                need_step = False
                continue
            home_odd = float(tr.xpath('td')[0].xpath('text()').extract()[0])
            draw_odd = float(tr.xpath('td')[1].xpath('text()').extract()[0])
            away_odd = float(tr.xpath('td')[2].xpath('text()').extract()[0])
            try:
                update_time = datetime.datetime.strptime(tr.xpath('td')[3].xpath('text()').extract()[0].replace('(Early)',''), '%d-%m-%Y %H:%M')
            except:
                print('update_time 出错')
            update_mktime = time.mktime(update_time.timetuple())

            odd_Item = OddSpiderItem()
            odd_Item['league_name'] = league_name   # str
            odd_Item['match_id'] = match_id     # str
            odd_Item['home_name'] = home_name   # str
            odd_Item['away_name'] = away_name   # str
            odd_Item['start_time'] = time.strftime("%Y-%m-%d %H:%M", time.localtime(start_mktime))  # str
            odd_Item['half_match_result'] = half_match_result       # int型
            odd_Item['match_result'] = match_result     # int型
            odd_Item['company_id'] = company_id     # str
            odd_Item['company_name'] = company_name     # str
            odd_Item['home_odd'] = home_odd     # float型
            odd_Item['draw_odd'] = draw_odd     # float型
            odd_Item['away_odd'] = away_odd     # float型
            odd_Item['update_time'] = time.strftime("%Y-%m-%d %H:%M", time.localtime(update_mktime))    # str
            odd_Item['count_index'] = count_index   # int
            odd_Item['current_search_date'] = current_search_date     # str 例：2018_01_10
            count_index += 1

            yield odd_Item










