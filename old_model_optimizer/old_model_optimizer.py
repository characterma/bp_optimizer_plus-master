#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

sys.path.append('utils')
sys.path.append('old_model_optimizer')
sys.path.append('grpc\\proto')
sys.path.append('old_model_optimizer\\grpc2\\proto')

from andun_sql.andun_sql import AndunSql
from config import DB_TYPE
from wx_opt import opt_bp_wx
from utils import send_ding_message
import pandas as pd
import numpy as np
import time
from log import MyLogger
import datetime


# old_model 的日志
old_model_log = MyLogger('./log/old_model.log', level='info')


def prepare_data(ansql, wear_user_id):
    calibrate_sql = 'SELECT wear_user_id,gmt_create,dbp,sbp,create_time,status FROM andun_cms.c_bp_history WHERE wear_user_id="{}" AND status=0;'.format(wear_user_id)
    calibrate_data = ansql.ansql_read_mysql(calibrate_sql)

    if calibrate_data.empty==True:
        return None, None

    # 从gmt_create提取date，time，create_date
    calibrate_data['create_date'] = calibrate_data['create_time'].apply(lambda x: x.strftime('%Y%m%d'))
    calibrate_data['date'] = calibrate_data['gmt_create'].apply(lambda x: x.strftime('%Y%m%d'))
    calibrate_data['time'] = calibrate_data['date'] + calibrate_data['gmt_create'].apply(lambda x: x.strftime('%H%M%S'))
    calibrate_data['time'] = calibrate_data['time'].apply(lambda x: int(x))
    calibrate_data['sbp'] = calibrate_data['sbp'].apply(lambda x: int(x))
    calibrate_data['dbp'] = calibrate_data['dbp'].apply(lambda x: int(x))

    # 查询优化记录
    opt_sql = "SELECT wear_user_id,wear_user_special_bp,model_version,calibration_time FROM andun_watch.d_special_bp WHERE WEAR_USER_ID = '{}';".format(wear_user_id)
    opt_data = ansql.ansql_read_mysql(opt_sql)

    # 数据按时间排序
    opt_data.sort_values(by='calibration_time', ascending=False, inplace=True)
    calibrate_data.sort_values(by='time', ascending=False, inplace=True)
    return calibrate_data, opt_data


def opt_bp(user, last_date, predict_data, real_data, opt_data, flag):
    # opt_result = []

    # flag 表示此用户是否成功经过优化
    flag = True

    #时间点详尽与分段还原血压模型出值
    try:
        tempt_date0 = last_date.replace('-', '')
        predict_data['time'] = predict_data['time'].apply(lambda x: int(tempt_date0 + str(x)))
        opt_data['time'] = opt_data['calibration_time'].apply(lambda x: int(x.strftime('%Y%m%d%H%M%S')))

        opt_time = opt_data['time'].to_list()

        # 分段还原模型真实出值
        opt_time.sort(reverse=True)
        try:
            t0 = int(last_date + '000000') + 1000000
            opt_data.sort_values(by='time', ascending=False, inplace=True)
            opt_data.reset_index(drop=True, inplace=True)
        except:
            pass
        for n, t in enumerate(opt_time):
            try:
                bool_index = (predict_data.time >= t) & (predict_data.time < t0)
                bia = opt_data.at[n, 'wear_user_special_bp'].split(',')
                sbp_values = predict_data[bool_index].sbp.to_list()
                sbp_values = [i - int(bia[0]) for i in sbp_values]
                dbp_values = predict_data[bool_index].dbp.to_list()
                dbp_values = [i - int(bia[1]) for i in dbp_values]
                predict_data.loc[bool_index, 'sbp'] = np.array(sbp_values)
                predict_data.loc[bool_index, 'dbp'] = np.array(dbp_values)
                t0 = t
            except:
                continue
    except:
        pass

    real_data.sort_values(by='time', ascending=True, inplace=True)

    #找出客服记录的当天用户真实血压值对应的最早,最晚时间
    first_time = real_data["time"].to_list()[0]
    last_time = real_data["time"].to_list()[-1]

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
        pd_df_cut.loc[pd_df_cut["sbp"] == pd_df_cut.loc[:,"sbp"].max(),"sbp"] = pd_df_cut['sbp'].mean()
        pd_df_cut.loc[pd_df_cut["dbp"] == pd_df_cut.loc[:,"dbp"].max(),"dbp"] = pd_df_cut['dbp'].mean()

    ## 重新计算均值
    # 后台模型的血压的均值
    p_sbp_mean = pd_df_cut['sbp'].mean()
    p_dbp_mean = pd_df_cut['dbp'].mean()

    # 客服记录的此时间段的血压均值
    r_sbp_mean = real_data['sbp'].mean()
    r_dbp_mean = real_data['dbp'].mean()

    opt_sbp = r_sbp_mean - p_sbp_mean
    opt_dbp = r_dbp_mean - p_dbp_mean

    # try:
    #     last_opt_model = opt_data['model_version'].to_list()[0]
    # except:
    #     last_opt_model = '1'

    
    
    # opt_result.append(user)
    opt_result = [int(opt_sbp), int(opt_dbp)]
    # opt_result.append(last_opt_model)
    # opt_result.append(time.strftime('%Y-%m-%d %H:%M:%S'))

    # opt_result.append(real_data['sbp'].to_list())
    # opt_result.append(int(real_data['sbp'].mean()))
    # opt_result.append(real_data['dbp'].to_list())
    # opt_result.append(int(real_data['dbp'].mean()))

    # opt_result.append(predict_data['sbp'].to_list())
    # opt_result.append(int(predict_data['sbp'].mean()))
    # opt_result.append(predict_data['dbp'].to_list())
    # opt_result.append(int(predict_data['dbp'].mean()))

    return flag, opt_result


def auto_optimizer(wear_user_id, opt_status):

    """
    旧模型优化血压 脚本入口
    """

    ansql = AndunSql(DB_TYPE)
    # calibrate_data: 用户显示的结果值， opt_data: 用户的历史校准值
    calibrate_data, opt_data = prepare_data(ansql, wear_user_id)

    # 获取最新模型版本
    try:
        mod_version = opt_data.model_version.values[0]
    except:
        mod_version = '1'

    # 判断客服数据是否为空（删除情况）空则初始化
    if calibrate_data is None:
        ansql.insert_into_bp_special(wear_user_id, '0,0', mod_version, time.strftime('%Y-%m-%d %H:%M:%S'))
        old_model_log.logger.info("user: {}\n无有效数据，校准值初始化".format(wear_user_id))
        return

    # 截取当日客服测量血压数据
    calibrate_data = calibrate_data[calibrate_data.create_date == calibrate_data.create_date.to_list()[0]]

    # 判断是否为武轩模型调过来的用户
    if opt_status == 1:
        flag, opt_sbp, opt_dbp, mod_version= opt_bp_wx(wear_user_id, calibrate_data, '1')
        if flag:
            ansql.insert_into_bp_special(wear_user_id, ','.join([str(int(opt_sbp)),str(int(opt_dbp))]), mod_version, time.strftime('%Y-%m-%d %H:%M:%S'))
            send_ding_message('wear_user_id: {} --> From WX BP optimize success...'.format(wear_user_id))
            return
        else:
            send_ding_message('wear_user_id: {} --> From WX BP optimize Failed...'.format(wear_user_id))
            return

    # 判断客服记录是否有换模型之前数据
    if mod_version != '1' and opt_data.empty==False:
        t_opt = opt_data[opt_data.model_version == mod_version].calibration_time.values[-1]
        if calibrate_data.gmt_create.values[-1] <= t_opt:
            flag, opt_sbp, opt_dbp, mod_version= opt_bp_wx(wear_user_id, calibrate_data, mod_version)
            if flag:
                ansql.insert_into_bp_special(wear_user_id, ','.join([str(int(opt_sbp)),str(int(opt_dbp))]), mod_version, time.strftime('%Y-%m-%d %H:%M:%S'))
                send_ding_message('wear_user_id: {} --> Cross Model optimize success...'.format(wear_user_id))
                return
            else:
                send_ding_message('wear_user_id: {} --> Cross Model optimize Failed...'.format(wear_user_id))
                return
        
    predict_sql = 'SELECT WEAR_USER_ID,blood_pressure,ABNORMAL_blood_pressure,DATE FROM andun_watch.d_bp_data WHERE WEAR_USER_ID = "{}" AND DATE = "{}";'

    # 获取客服录入血压值校准依赖日列表
    date_list = calibrate_data.date.unique()

    # flag用户是否成功经过校准
    flag = False

    # 遍历计算校准值，结果平均
    opt_sbps = []
    opt_dbps = []
    for date in date_list:

        #获取当日录入真实血压值
        real_data = calibrate_data[calibrate_data['date'] == date]

        # 查询此用户同一天的后台模型的预测值
        sql = predict_sql.format(wear_user_id, date)
        predict_data = ansql.ansql_read_mysql(sql)

        if predict_data.empty == True:
            continue

        p_results = []
        if predict_data.shape[0] > 0:
            bp_str = predict_data['blood_pressure'].to_list()[0]
            for spot in bp_str.split(";"):
                temp = []
                temp.append(wear_user_id)
                spot_list = list(spot.split(","))
                temp.append(spot_list[0])
                temp.append(int(spot_list[1]))
                temp.append(int(spot_list[2]))
                temp.append(date)
                p_results.append(temp)
        
            # 创建dataframe
            predict_data = pd.DataFrame(data=p_results, columns=['wear_user_id', 'time', 'sbp', 'dbp', 'date'])
            
            # 开始计算校准值
            flag, opt_result = opt_bp(wear_user_id, date, predict_data, real_data, opt_data, flag)

            opt_sbps.append(opt_result[0])
            opt_dbps.append(opt_result[1])
        
        else:
            continue
        
    # 均值
    opt_sbp = np.mean(opt_sbps)
    opt_dbp = np.mean(opt_dbps)

        # 判断 flag, 执行 插入 d_special_bp
    if  flag:
        ansql.insert_into_bp_special(wear_user_id, ','.join([str(int(opt_sbp)),str(int(opt_dbp))]), mod_version, time.strftime('%Y-%m-%d %H:%M:%S'))
        # pass
    else:
        old_model_log.logger.info("user: {},用户提交数据对应日期无有效手表出值，无法校准".format(wear_user_id))
        
    # # opt_results创建dataframe
    # opt_results_df = pd.DataFrame(data=[temp_opt_result], columns=['wear_user_id', 'wear_user_special_bp', 'model_version', 'calibration_time', 'real_sbp', 'real_sbp_mean', 'real_dbp', 'real_dbp_mean', 'pred_sbp', 'pred_sbp_mean', 'pred_dbp', 'pred_dbp_mean'])
    # opt_results_df.to_csv("opt_results_{}.csv".format(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H-%M-%S')), index=False, encoding='utf8')
    # print(opt_results_df)

if __name__ == "__main__":
    # wear_user_id = '49238111'
    wear_user_id = 'E9MJ292P'
    auto_optimizer(wear_user_id,0)