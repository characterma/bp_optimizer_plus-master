import sys
import json
import grpc
from numpy.core.fromnumeric import mean
# import datetime
# import time
import pandas as pd
import numpy as np

from andun_sql.andun_sql import AndunSql
from grpc2.proto import anbp_pb2_grpc, anbp_pb2
# from log import MyLogger



def opt_bp_wx(user, real_data, Mod_version):
    # flag此用户是否成功经过优化
    flag = True

    uid = user

    # 旧模型 NG 的地址
    IP = "47.95.230.250:50056" 

    ansql = AndunSql()

    # 查询 此用户的基本信息
    user_info = ansql.ansql_user_info(uid)
    age = user_info['Age'].tolist()[0]
    gender = user_info['Gender'].tolist()[0]
    height = user_info['Height'].tolist()[0]
    weight = user_info['Weight'].tolist()[0]
    user_info = {
        "wearUserId": uid,
        "age": age,
        "height": height,
        "weight": weight,
        "gender": gender
    }
    user_info = json.dumps(user_info)

    # 保存 每一组 ppg_feature 的预测结果
    predict_data = []

    # 获取预测时间列表
    date_list = real_data['date'].unique()


    # 遍历 每个 ppg_value数据
    opt_sbps = []
    opt_dbps = []

    channel = grpc.insecure_channel(IP)
    stub = anbp_pb2_grpc.GreeterStub(channel=channel)

    for date in date_list:

        last_sbp = None
        last_dbp = None
        last_cli = None
        last_time = 0
        time_differ_minute = None

        temp_dft = ansql.ansql_bp_feature(uid, [date])

        # 判断当天是否有数据
        if temp_dft.shape[0] == 0:
            continue

         # 遍历当天的每条 ppg_value 数据
        for a in range(temp_dft.shape[0]):
            # 第一次调用的时候
            if a == 0:
                last_sbp = 130
                last_dbp = 80
                last_cli = 100
                time_differ_minute = 32
            else:
                # 计算 time_differ_minute
                time_differ_minute = int(temp_dft.iloc[a]['ppg_time']) - int(last_time)
                time_differ_minute = int(abs(time_differ_minute / 60))

            ppg_values_all = temp_dft.iloc[a]['FROMPPG']

            for ps in ppg_values_all.split(";"):
                if len(ps) == 0:
                        continue
                ppg_time, ppg_values = ps.split("/", 1)
                # 把ppg_values保存成 dataframe
                temp_result = []
                
                ##################################
                ##################################
                # 调用server
                
                response = stub.ANBP(anbp_pb2.BPRequest(userInfo = user_info,
                                                        oStatus = Mod_version,
                                                        features=ppg_values,
                                                        preReportBP="{},{},{}".format(last_sbp, last_dbp, last_cli),
                                                        #  preReportBP="0,-1",
                                                        preReportTimeDiffMinute = "{}".format(time_differ_minute)))

                pred_result = response.bloodPpressure

                if pred_result:
                    pred_result = pred_result.split("/")
                    temp_result.append(int(pred_result[0]))
                    temp_result.append(int(pred_result[1]))

                    # 更新 上一次的预测数据
                    last_sbp = pred_result[0]
                    last_dbp = pred_result[1]
                    last_cli = pred_result[2]
                    last_time = ppg_time
                else:
                    temp_result.append(None)
                    temp_result.append(None)

                temp_result.append(date + ppg_time)

                predict_data.append(temp_result)
        predict_data = pd.DataFrame(data=predict_data, columns=['sbp', 'dbp',  'time'])

        # 格式化数据
        # predict_data.time.astype('int')

        # 客服记录当天数据
        real_data_day = real_data[real_data['date'] == date]

        #找出客服记录的当天用户真实血压值对应的最早,最晚时间
        first_time = real_data_day["time"].to_list()[-1]
        last_time = real_data_day["time"].to_list()[0]

        # 按照 最早,最晚时间,找出 模型预测值的 最近的时间点
        predict_data['delta_ft'] = predict_data['time'].apply(lambda x: abs(int(x) - int(first_time)))
        predict_data['delta_lt'] = predict_data['time'].apply(lambda x: abs(int(x) - int(last_time)))

        # 按照最大最小值截取
        pd_first_time = predict_data[predict_data['delta_ft'] == predict_data['delta_ft'].min()]['time'].to_list()[0]
        pd_last_time = predict_data[predict_data['delta_lt'] == predict_data['delta_lt'].min()]['time'].to_list()[0]

        pd_df_cut = predict_data[predict_data['time'] >= pd_first_time]
        pd_df_cut = pd_df_cut[pd_df_cut['time'] <= pd_last_time]

        # 替换 后台数据中的最大,最小值, 记录长度大于5的时候, 作用是去除一些异常值
        if pd_df_cut.shape[0] > 5:
            a = pd_df_cut['sbp'].mean()
            a1 = pd_df_cut['sbp']
            a2 = mean(a1)
            # 均值替换sbp, dbp的最大值
            pd_df_cut.loc[pd_df_cut["sbp"] == pd_df_cut.loc[:,"sbp"].max(),"sbp"] = pd_df_cut['sbp'].mean()
            pd_df_cut.loc[pd_df_cut["dbp"] == pd_df_cut.loc[:,"dbp"].max(),"dbp"] = pd_df_cut['dbp'].mean()

        ## 重新计算均值
        # 后台模型的血压的均值
        p_sbp_mean = pd_df_cut['sbp'].mean()
        p_dbp_mean = pd_df_cut['dbp'].mean()

        # 客服记录的此时间段的血压均值
        r_sbp_mean = real_data_day['sbp'].mean()
        r_dbp_mean = real_data_day['dbp'].mean()

        opt_sbp = r_sbp_mean - p_sbp_mean
        opt_dbp = r_dbp_mean - p_dbp_mean
        opt_result = [int(opt_sbp), int(opt_dbp)]

        opt_sbps.append(opt_result[0])
        opt_dbps.append(opt_result[1])

    opt_sbp = np.mean(opt_sbps)
    opt_dbp = np.mean(opt_dbps)

    return flag, opt_sbp, opt_dbp, Mod_version


