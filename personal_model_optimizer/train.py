#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
sys.path.append(r".")
sys.path.append(r"utils/")
sys.path.append(r'personal_model_optimizer/grpc/proto/')
sys.path.append(r"personal_model_optimizer/utils")
sys.path.append(r"old_model_optimizer")

import warnings
warnings.filterwarnings('ignore')

import time
from andun_sql.andun_sql import AndunSql
from config import Min_Minute, DB_TYPE

from a_1_select_kefu_data_by_user import select_kefu_data_by_user
from a_3_find_feature_bp_by_user import find_feature_bp_by_user
from assemble_bp_feature import assemble_bp_feature

import grpc
import anbp_pb2, anbp_pb2_grpc
from log import MyLogger
from old_model_optimizer import auto_optimizer


# train 的日志
train_log = MyLogger('./log/train.log', level='info')

# 线上 NG 的地址
# IP = "39.97.198.78:50057"

IP = "127.0.0.1:50057"

"""
按照选定的时间点, 筛查客服记录的数据里面, 血压数据比较多的用户,跑一下个人模型试试,看
是否满足要求,如果满足,则加入新模型
"""



def train(wear_user_id):
    # 调用线上的接口, 使用新数据重新训练
    user_info = '{"wearUserId": "' + wear_user_id + '","age": 26,"height": 180,"weight": 85,"gender": 1}'
    test_bp_features = ""
    channel = grpc.insecure_channel(IP)
    stub = anbp_pb2_grpc.GreeterStub(channel=channel)
    response = stub.ANBP(anbp_pb2.BPRequest(userInfo = user_info,
                                            oStatus = "-2",
                                            features=test_bp_features,
                                            preReportBP="100,90,90",
                                            #  preReportBP="0,-1",
                                            preReportTimeDiffMinute = "31"))

    print("status: {}, bloodPpressure: {}, timestamp: {}".format(response.status, response.bloodPpressure, response.timestamp))
    return response.status


def main_train(wear_user_id, opt_status, is_in_wx):
    ansql = AndunSql(db_type=DB_TYPE)

    # 查询此用户的所有 血压记录    
    sql_select = 'SELECT wear_user_id, gmt_create,create_time FROM andun_cms.c_bp_history WHERE wear_user_id = "{}" AND status=0'.format(wear_user_id)
    bp_history_data = ansql.ansql_read_mysql(sql_select)

    # 如果此用户的所有数据都删除了,没有数据
    if bp_history_data.shape[0] == 0:
        # 走 旧血压模型的校准
        ############################
        ############################
        auto_optimizer(wear_user_id, is_in_wx)
    else:
        # 从 d_users_bp_model 里面 找出使用新模型的用户
        sql_select_new_model_users = 'SELECT wear_user_id, create_time, update_time FROM andun_watch.d_users_bp_model'
        new_model_users = ansql.ansql_read_mysql(sql_select_new_model_users)
        new_model_users['wear_user_id'] = new_model_users['wear_user_id'].apply(lambda x: str(x))
        new_model_unique_users = new_model_users['wear_user_id'].unique()

        sql_user_info  = "SELECT ID,USERNAME FROM andun_app.a_wear_user WHERE ID = '{}' ".format(wear_user_id)
        user_info = ansql.ansql_read_mysql(sql_user_info)


        # 对此 wear_user_id 用户的数据进行判断
        uwu = wear_user_id

        # 搜集ppg特征值
        assemble_bp_feature(wear_user_id=wear_user_id)

        # 找出此用户的所有记录数据
        this_user_history = bp_history_data[bp_history_data['wear_user_id'] == uwu]

        if uwu in new_model_unique_users:
            # 找出此用户被加入新模型的时间
            # create_time = new_model_users[new_model_users['wear_user_id'] == uwu]['create_time'].tolist()[-1]
            update_time = new_model_users[new_model_users['wear_user_id'] == uwu]['update_time'].tolist()[-1]
            # 判断加入新模型后,此用户有没有上传血压数据
            this_user_history = this_user_history[this_user_history['gmt_create'] > update_time]
            train_log.logger.info(" {} 此用户已加入新血压模型, 加入新血压模型后, 上传了 {} 组血压数据...".format(uwu, len(this_user_history)))

            if len(this_user_history) > 0:
                status = train(uwu)
                if str(status) == '2':
                    # 修改 d_users_bp_model 的 update_time值
                    new_update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    ansql.update_d_users_bp_model(uwu, new_update_time)
                    train_log.logger.info("{} 已重新训练...".format(uwu))
                else:
                    train_log.logger.error("{} 重新训练 失败...".format(uwu))
                    # 删除
                    ansql.delete_d_users_bp_model(uwu)
            else:
                train_log.logger.info("{} 不需要重新训练 ...".format(uwu))

        else:
            # 此用户不在新模型里面, 统计此用户记录数据的数量
            this_user_history.sort_values(by=['gmt_create'], ascending=True, inplace=True)
            # 找出记录数据的 最早的时间
            start_time = this_user_history['gmt_create'].tolist()[0]
            # 找出记录数据的 最晚的时间
            end_time = this_user_history['gmt_create'].tolist()[-1]

            if len(this_user_history) >= 10:
                train_log.logger.info(" {} 此用户不在新血压模型中, 共记录了 {} 条数据, 最早时间: {}, 最晚时间: {}".format(uwu, len(this_user_history), start_time, end_time))
                # 尝试对此用户进行个人模型建模
                # 查询此用户的客服记录的血压值记录
                select_kefu_data_by_user(wear_user_id=uwu)
                # 根据客服记录的用户测量的每次的血压时间, 找出同一天手环 里面最接近 对应时间点的ppg信号数据
                try:
                    un_sbp, un_dbp = find_feature_bp_by_user(wear_user_id=uwu, min_minute=Min_Minute)
                except Exception as e:
                    train_log.logger.error("{} 匹配客服记录的数据发生错误...{}".format(uwu, e))

                try:
                    if len(un_sbp) > 10:
                        train_log.logger.info(" {} 此用户可用的血压数据数量...{}".format(uwu, len(un_sbp)))
                        train_log.logger.info("调用个人血压模型接口开始训练...")
                        time.sleep(5)
                        # 调用线上模型接口进行训练
                        status = train(uwu)
                        if str(status) == '2':
                            new_update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            ansql.insert_d_users_bp_model(uwu, new_update_time)
                            train_log.logger.info(" {} 已加入新模型...".format(uwu))
                        else:
                            train_log.logger.error(" {} 加入新模型失败...".format(uwu))

                        train_log.logger.info("数据个数-{}, 训练结果-{}".format(len(un_sbp), status))
                    else:
                        train_log.logger.info("数据个数-{}".format(len(un_sbp)))
                        # 走 旧血压模型的校准
                        ############################
                        ############################
                        auto_optimizer(wear_user_id, is_in_wx)
                    

                except Exception as e:
                    train_log.logger.info("{} 数据处理及模型训练错误! {}".format(uwu, e))
            
            else:
                # 走 旧血压模型的校准
                ############################
                ############################
                auto_optimizer(wear_user_id, is_in_wx)