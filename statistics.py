import pymysql.cursors
import datetime
import json
import pdb
from collections import Counter

info_days = 30  # 收集多少天的信息

search_date = []
for i in range(info_days):
    if datetime.datetime.now().hour < 8:
        add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 2))).strftime("%Y-%m-%d")
    else:
        add_day = (datetime.datetime.now() + datetime.timedelta(days=-(i + 1))).strftime("%Y-%m-%d")
    search_date.append(add_day)


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

current_search_year_month_dict = {}     # 根据月份分隔的查询数据的信息字典

try:
    with connection.cursor() as cursor:
        current_serach_year_month = ''
        for current_search_date in search_date:
            # 获取当前查询日期的年月，方便分割统计
            current_search_year = current_search_date.split('-')[0]
            current_search_month = current_search_date.split('-')[1]
            current_serach_year_month = current_search_year + '_' + current_search_month
            if not current_serach_year_month in current_search_year_month_dict.keys():
                current_search_year_month_dict[current_serach_year_month] = {}

            tableName = 'history_analysis_' + current_search_date.replace('-', '_')  # 表名
            search_sql = 'SELECT max_accurate_company,min_accurate_company,current_rate FROM %s'
            try:
                cursor.execute(search_sql % tableName)
            except:
                print('没找到')
                continue
            get_company_list = cursor.fetchall()

            # 每日信息
            max_accurate_company_dict = {}
            min_accurate_company_dict = {}
            avarage_accurate_company_dict = {}
            total_avarage_accurate_company_dict = {}
            total_current_rate = 0
            total_bet_num = 0
            for single_company in get_company_list:
                total_current_rate += single_company['current_rate']
                if single_company['current_rate'] != 0:
                    total_bet_num += 1
                if single_company['max_accurate_company'] in avarage_accurate_company_dict.keys():
                    # max_accurate_company_dict[single_company['max_accurate_company']] += 1
                    avarage_accurate_company_dict[single_company['max_accurate_company']] += 1
                    total_avarage_accurate_company_dict[single_company['max_accurate_company']] += 1
                else:
                    avarage_accurate_company_dict[single_company['max_accurate_company']] = 0
                    total_avarage_accurate_company_dict[single_company['max_accurate_company']] = 0
                if single_company['min_accurate_company'] in avarage_accurate_company_dict.keys():
                    # min_accurate_company_dict[single_company['min_accurate_company']] += 1
                    avarage_accurate_company_dict[single_company['min_accurate_company']] -= 1
                    total_avarage_accurate_company_dict[single_company['min_accurate_company']] += 1
                else:
                    avarage_accurate_company_dict[single_company['min_accurate_company']] = 0
                    total_avarage_accurate_company_dict[single_company['min_accurate_company']] = 0

            if 'total_current_rate' in current_search_year_month_dict[current_serach_year_month].keys():
                current_search_year_month_dict[current_serach_year_month]['total_current_rate'] += total_current_rate
            else:
                current_search_year_month_dict[current_serach_year_month]['total_current_rate'] = total_current_rate
            if 'total_bet_num' in current_search_year_month_dict[current_serach_year_month].keys():
                current_search_year_month_dict[current_serach_year_month]['total_bet_num'] += total_bet_num
            else:
                current_search_year_month_dict[current_serach_year_month]['total_bet_num'] = total_bet_num
            if 'avarage_accurate_company_dict' in current_search_year_month_dict[current_serach_year_month].keys():
                current_search_year_month_dict[current_serach_year_month]['avarage_accurate_company_dict'] = dict(Counter(current_search_year_month_dict[current_serach_year_month]['avarage_accurate_company_dict'])+Counter(avarage_accurate_company_dict))
            else:
                current_search_year_month_dict[current_serach_year_month]['avarage_accurate_company_dict'] = avarage_accurate_company_dict
            if 'total_avarage_accurate_company_dict' in current_search_year_month_dict[current_serach_year_month].keys():
                current_search_year_month_dict[current_serach_year_month]['total_avarage_accurate_company_dict'] = dict(Counter(current_search_year_month_dict[current_serach_year_month]['total_avarage_accurate_company_dict'])+Counter(total_avarage_accurate_company_dict))
            else:
                current_search_year_month_dict[current_serach_year_month]['total_avarage_accurate_company_dict'] = total_avarage_accurate_company_dict

        with open('statistics.txt', 'w', encoding='UTF-8') as f:
            for single_key in current_search_year_month_dict.keys():
                high_accurate_company_list = []
                f.write('当前查询年月：' + single_key)
                f.write('\n')
                if 'total_current_rate' in current_search_year_month_dict[single_key].keys():
                    total_current_rate_text = '当前总分: ' + str(current_search_year_month_dict[single_key]['total_current_rate']) + ' '
                else:
                    total_current_rate_text = '当前总分: '
                if 'total_current_rate' in current_search_year_month_dict[single_key].keys():
                    total_current_bet_text = '当前总下注: ' + str(current_search_year_month_dict[single_key]['total_bet_num'])
                else:
                    total_current_bet_text = '当前总下注: '
                f.write(total_current_rate_text)
                f.write(total_current_bet_text)
                f.write('\n')

                # key是公司名称
                for key in current_search_year_month_dict[single_key]['avarage_accurate_company_dict'].keys():
                    if current_search_year_month_dict[single_key]['total_avarage_accurate_company_dict'][key] != 0:
                        accurate_rate = round(((current_search_year_month_dict[single_key]['total_avarage_accurate_company_dict'][key] + current_search_year_month_dict[single_key]['avarage_accurate_company_dict'][key])/2)/current_search_year_month_dict[single_key]['total_avarage_accurate_company_dict'][key], 2)
                        # total_avarage_accurate_company_dict_text = key + '_命中率: ' + str(accurate_rate)
                    else:
                        accurate_rate = 0
                        # total_avarage_accurate_company_dict_text = key + '_无'
                    # f.write(total_avarage_accurate_company_dict_text)
                    # f.write('\n')
                    # avarage_accurate_company_text = key + ': ' + str(avarage_accurate_company_dict[key])
                    # f.write(avarage_accurate_company_text)
                    # f.write('\n')

                    if accurate_rate >= 0.50 and current_search_year_month_dict[single_key]['avarage_accurate_company_dict'][key] > 0:
                        if key in [item['name'] for item in high_accurate_company_list]:
                            key_index = [item['name'] for item in high_accurate_company_list].index(key)
                            high_accurate_company_list[key_index]['accurate_rate'] = (high_accurate_company_list[key_index]['accurate_rate'] + accurate_rate)/2
                            high_accurate_company_list[key_index]['avarage_accurate'] = (high_accurate_company_list[key_index]['avarage_accurate'] + current_search_year_month_dict[single_key]['avarage_accurate_company_dict'][key])/2
                        else:
                            high_accurate_company_dict = {
                                'name': key,
                                'accurate_rate': accurate_rate,
                                'avarage_accurate': current_search_year_month_dict[single_key]['avarage_accurate_company_dict'][key]
                            }
                            high_accurate_company_list.append(high_accurate_company_dict)
                current_search_year_month_dict[single_key]['high_accurate_company_list'] = high_accurate_company_list   # 保存当月的高准确率公司列表
                for high_accurate_company in high_accurate_company_list:
                    high_accurate_company_text = high_accurate_company['name'] + ' ' + str(high_accurate_company['accurate_rate']) + ' ' + str(high_accurate_company['avarage_accurate'])
                    f.write(high_accurate_company_text)
                    f.write('\n')
                high_accurate_company_pre_text = '高准确公司名称列表: '
                f.write(high_accurate_company_pre_text)
                for high_accurate_company in high_accurate_company_list:
                    high_accurate_company_text = '"' + high_accurate_company['name'] + '", '
                    f.write(high_accurate_company_text)
                # for key in min_accurate_company_dict.keys():
                #     min_accurate_company_text = key + ': ' + str(min_accurate_company_dict[key])
                #     f.write(min_accurate_company_text)
                #     f.write('\n')
                f.write('\n')
                f.writelines('------------------------------')
                f.write('\n')
            # 遍历search_year_month,找到共同的高准确率公司
            current_accurate_company_set = {}
            for single_key in current_search_year_month_dict.keys():
                current_accurate_company_list = [item['name'] for item in current_search_year_month_dict[single_key]['high_accurate_company_list']]
                if current_accurate_company_set == {}:
                    current_accurate_company_set = set(current_accurate_company_list)
                else:
                    current_accurate_company_set = (current_accurate_company_set & set(current_accurate_company_list))
            total_accurate_company_list = list(current_accurate_company_set)
            total_accurate_company_list_pre_text = '共同准确公司：'
            f.write('\n')
            f.write('=====================================')
            f.writelines(total_accurate_company_list_pre_text)
            f.write('\n')
            for total_high_accurate_company in total_accurate_company_list:
                high_accurate_company_text = '"' + total_high_accurate_company + '", '
                f.write(high_accurate_company_text)

    # connection is not autocommit by default. So you must commit to save your changes.
    cursor.close()
    if not connection.commit():
        connection.rollback()

finally:
    connection.close()