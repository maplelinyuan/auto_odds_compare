# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join

# 单个赔率 item
class OddSpiderItem(scrapy.Item):
    league_name = scrapy.Field()  # 联赛名称
    match_id = scrapy.Field()  # 比赛ID
    home_name = scrapy.Field()  # 主队名称
    away_name = scrapy.Field()  # 客队名称
    start_time = scrapy.Field()  # 开赛时间
    half_match_result = scrapy.Field()  # 半场比赛结果(310)
    match_result = scrapy.Field()  # 比赛结果(310)
    company_id = scrapy.Field()  # 当前赔率公司ID
    company_name = scrapy.Field()  # 当前赔率公司名称
    home_odd = scrapy.Field()  # 主胜赔率
    draw_odd = scrapy.Field()  # 平局赔率
    away_odd = scrapy.Field()  # 客胜赔率
    update_time = scrapy.Field()  # 赔率更新时间
    count_index = scrapy.Field()  # 赔率行index, 因为有些更新时间完全相同，所以需要这个来正确排序，最新的时间为0，依次递增
    current_search_date = scrapy.Field()  # 当前查询日期 用来建表

class OddSpiderLoader(ItemLoader):
    default_item_class = OddSpiderItem
    default_input_processor = MapCompose(lambda s: s.strip())
    default_output_processor = TakeFirst()
    description_out = Join()
