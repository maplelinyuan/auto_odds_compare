# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql.cursors
import datetime
import json
import pdb

nowadays = datetime.datetime.now().strftime('%Y_%m_%d')
nowatime = datetime.datetime.now().strftime('%Y_%m_%d_%H%M')

class AutoOddsComparePipeline(object):
    def process_item(self, item, spider):
        if spider.name == 'auto_odds_compare':
        # 这里写爬虫 auto_odds_compare 的逻辑
            if item['match_result'] == '':
                debugging = False
            else:
                debugging = True
            # 获取查询日期
            current_search_date = item['current_search_date']
            # Connect to the database
            db_name = 'auto_odds_compare'
            config = {
                'host': '127.0.0.1',
                'user': 'root',
                'password': '19940929',
                'db': db_name,
                'charset': 'utf8mb4',
                'cursorclass': pymysql.cursors.DictCursor
            }
            connection = pymysql.connect(**config)
            print('连接至数据库' + db_name)
            try:
                with connection.cursor() as cursor:
                    if debugging:
                        tableName = 'history_analysis_' + current_search_date.replace('-', '_')  # 表名
                    else:
                        tableName = 'analysis_' + current_search_date.replace('-', '_')  # 表名
                    # 建立当前队伍表
                    build_table = (
                        "CREATE TABLE IF NOT EXISTS "' %s '""
                        "(id INT auto_increment NOT NULL PRIMARY KEY,"
                        "league_name VARCHAR(30) NOT NULL,"
                        "home_name VARCHAR(30) NOT NULL,"
                        "away_name VARCHAR(30) NOT NULL,"
                        "start_time VARCHAR(30) NOT NULL,"
                        "match_result VARCHAR(8) NOT NULL,"
                        "max_accurate_company VARCHAR(30) NOT NULL,"
                        "min_accurate_company VARCHAR(30) NOT NULL,"
                        "current_rate FLOAT(8) NOT NULL,"
                        "support_list_text VARCHAR(30) NOT NULL)"
                    )
                    cursor.execute(build_table % tableName)
                    # 建表完成
                    insert_sql = (
                            "INSERT INTO " + tableName + " VALUES "
                                                         "(%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', %f, '%s')"
                    )
                    # 如果非debugging则需要更新
                    if item['match_result'] == '':
                        cursor.execute(
                            'SELECT id FROM %s  WHERE home_name="%s" and away_name="%s"' % (tableName, item['home_name'], item['away_name']))
                        table_row_len = len(cursor.fetchall())
                        print('analysis 表中存在查询数据的数目：:', table_row_len)
                        if table_row_len < 1:
                            try:
                                print('insert数据库')
                                cursor.execute(insert_sql % (
                                    0,
                                    item['league_name'], item['home_name'], item['away_name'], item['start_time'],
                                    item['match_result'],
                                    item['max_accurate_company'], item['min_accurate_company'],
                                    item['current_rate'], item['support_list_text']))
                            except Exception as e:
                                print("数据库执行失败 ", e)
                        else:
                            try:
                                print('update信息')
                                update_sql = (
                                    'UPDATE %s SET support_list_text="%s" WHERE home_name="%s" and away_name="%s"'
                                )
                                cursor.execute(update_sql % (
                                    tableName, item['support_list_text'], item['home_name'],
                                    item['away_name']))
                            except Exception as e:
                                print("数据库执行失败 ", e)
                    else:
                        cursor.execute(
                            'SELECT id FROM %s  WHERE home_name="%s" and away_name="%s"' % (
                            tableName, item['home_name'], item['away_name']))
                        table_row_len = len(cursor.fetchall())
                        print('analysis 表中存在查询数据的数目：:', table_row_len)
                        if table_row_len < 1:
                            try:
                                print('insert数据库')
                                # 打开chinese2english, 将之前保存的首发信息中的名称转换为英文再与当前odd列表中的信息进行模糊匹配找出那场比赛，进行计算
                                # with open('auto_odds_compare/english2chinese.json', 'r', encoding='utf-8') as json_file:
                                #     english2chinese = json.load(json_file)
                                # chinese_league_name = english2chinese[item['league_name']]['name']
                                cursor.execute(insert_sql % (
                                    0,
                                    item['league_name'], item['home_name'], item['away_name'], item['start_time'],
                                    item['match_result'],
                                    item['max_accurate_company'], item['min_accurate_company'],
                                    item['current_rate'], item['support_list_text']))
                            except Exception as e:
                                print("数据库执行失败 ", e)

                # connection is not autocommit by default. So you must commit to save your changes.
                cursor.close()
                if not connection.commit():
                    connection.rollback()

            finally:
                connection.close()
        return item
