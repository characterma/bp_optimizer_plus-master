#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
sys.path.append(r"utils/")
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
from andun_sql.andun_sql import AndunSql
from config import DB_TYPE

"""
把客服记录血压数据, 按照每个用户, 每天, 从bp_feature表里面查找, 4月26号前的, 查旧表, 以后的查新表
把查找到的ppg feature, 保存到一个单独的表中, 用于训练模型
"""


def assemble_bp_feature(wear_user_id):
    ansql = AndunSql(db_type=DB_TYPE)

    sql_select = 'SELECT wear_user_id,gmt_create,create_time FROM andun_cms.c_bp_history WHERE wear_user_id="{}" '.format(wear_user_id)
    bp_history_data = ansql.ansql_read_mysql(sql_select)

    # for uu in tqdm(un_users):
    uu = wear_user_id
    # 找出此用户的所有血压记录
    temp_history = bp_history_data[bp_history_data['wear_user_id'] == uu]
    # temp_history['days'] = temp_history['gmt_create'].apply(lambda x: time.strftime("%Y-%m-%d", x))
    temp_history['days'] = temp_history['gmt_create'].apply(lambda x: x.strftime("%Y-%m-%d"))

    un_days = temp_history['days'].unique()
    # un_days = ['2020-09-08', '2020-09-09', '2020-09-10', '2020-09-11', '2020-09-12']
    print(un_days)
    # 遍历每一天
    for und in un_days:
        # 先从新表里面查询 是否有此用户 的 当天的ppg数据
        sql_1 = "SELECT WEAR_USER_ID, DATE, FROMPPG FROM andun_watch.d_bp_feature_model WHERE WEAR_USER_ID='{}' AND DATE='{}' ".format(uu, und)
        temp = ansql.ansql_read_mysql(sql_1)

        # this_day_all_ppg = pd.DataFrame(columns=['ID','DEVICE_ID','WEAR_USER_ID', 'DATE', 'FROMPPG'])

        und = int(und.replace('-', ''))
        # 所有保存ppg数据的表, bp_feature,  bp_feature_1
        sql_ppg = "SELECT ID,DEVICE_ID,WEAR_USER_ID,DATE,FROMPPG FROM andun_watch.d_bp_feature WHERE WEAR_USER_ID='{}' AND DATE='{}' ".format(uu, und)
        # sql_ppg_1 = "SELECT ID,DEVICE_ID,WEAR_USER_ID,DATE,FROMPPG FROM andun_watch.d_bp_feature_1 WHERE WEAR_USER_ID='{}' AND DATE='{}' ".format(uu, und)
        # 判断 日期时间是否 小于 2020-09-09, 9月9号的时候,线上的生产 bp_feature 更换了表
        # 再查 线上生产数据库
        # ppg_3 = ansql.ansql_read_mysql(sql_ppg)
        # ppg_4 = ansql.ansql_read_mysql(sql_ppg_1)
        # for ppg_n in [ppg_3, ppg_4]:
        #     if ppg_n.shape[0] > 0:
        #         this_day_all_ppg = pd.concat([this_day_all_ppg, ppg_n], ignore_index=True)

        this_day_all_ppg = ansql.ansql_read_mysql(sql_ppg)

        if temp.shape[0] == 0:
            # 判断 this_day_all_ppg 的长度是否为1, 为1的话说明只在一个表里保存了数据, 直接写入目标数据表
            if this_day_all_ppg.shape[0] == 0:
                print(" {} - {} 没有当天的ppg数据......".format(uu, und))
            elif this_day_all_ppg.shape[0] == 1:
                print(" {} - {} 可以直接写入目标数据库......".format(uu, und))
                ansql.insert_into_bp_feature_model(this_day_all_ppg['ID'].values[0],
                                                this_day_all_ppg['FROMPPG'].values[0],
                                                this_day_all_ppg['DATE'].values[0],
                                                this_day_all_ppg['DEVICE_ID'].values[0],
                                                this_day_all_ppg['WEAR_USER_ID'].values[0]
                                                )
            else:
                print(" {} - {} 数据保存在多张表里......".format(uu, und))
                # print(this_day_all_ppg)
                ansql.insert_into_bp_feature_model(this_day_all_ppg['ID'].values[0],
                                                "".join(this_day_all_ppg['FROMPPG'].values),
                                                this_day_all_ppg['DATE'].values[0],
                                                this_day_all_ppg['DEVICE_ID'].values[0],
                                                this_day_all_ppg['WEAR_USER_ID'].values[0]
                                                )
        else:
            # 判断此用户是否有新产生的ppg特征数据,如果有的话需要进行更新
            if this_day_all_ppg.shape[0] > 0 and len(this_day_all_ppg['FROMPPG'].values[0]) > len(temp['FROMPPG'].values[0]):
                # 说明有新的ppg数据产生, 需要更新
                print(" {} - {} 更新ppg特征数据......".format(uu, und))
                ansql.update_bp_feature_model(this_day_all_ppg['ID'].values[0],
                                                this_day_all_ppg['FROMPPG'].values[0],
                                                this_day_all_ppg['DATE'].values[0],
                                                this_day_all_ppg['DEVICE_ID'].values[0],
                                                this_day_all_ppg['WEAR_USER_ID'].values[0]
                                                )

            else:
                print(" {} - {} 已经导出.......".format(uu, und))
