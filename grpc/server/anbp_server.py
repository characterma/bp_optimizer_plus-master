#!/usr/bin/python
# -*- coding: utf-8 -*-
from concurrent import futures
import time
import grpc
import sys

sys.path.append(".")
sys.path.append(r'grpc/')
sys.path.append(r"grpc/proto")
sys.path.append(r"utils/")
sys.path.append(r"personal_model_optimizer")

from log import MyLogger
import anbpOptimizer_pb2
import anbpOptimizer_pb2_grpc
import ssl
import time
from config import _ONE_DAY_IN_SECONDS, PORT

from utils import send_ding_message, is_in_wx_model, delete_user_from_wuxuan_model
import threading
from train import main_train


# 返回状态码定义
ANBP_RES_FAILD = '1'
ANBP_RES_SUCCEED = '0'

# grpc_server的日志
bp_server_log = MyLogger('./log/server.log', level='info')


try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context



def optimizer(wear_user_id, opt_status):
    # 判断此用户是否在武轩的模型中
        res = is_in_wx_model(wear_user_id)
        time.sleep(1)
        if res == 1:
            bp_server_log.logger.info('{} 也在武轩的模型中, 执行删除...'.format(wear_user_id))
            # 1 从武轩的模型删除
            delete_user_from_wuxuan_model(wear_user_id=wear_user_id)
            bp_server_log.logger.info('{} 删除成功...'.format(wear_user_id))
        
        try:
            # 2 看此用户是否在个人血压模型里面, 如果在则更新模型, 如果不在进入下一步判断
            # 3 判断此用户提供的数据是否满足个人血压模型的条件, 有效数据是否多于 10条 , 判断的时候注意从 c_bp_history 里的查询条件
            # 3.1 满足个人血压模型: 收集ppg特征数据, 修改
            # 3.2 不满足个人血压模型的话, 走旧模型校准
            main_train(wear_user_id, opt_status, res)
            # 发送消息到钉钉群
            send_ding_message('wear_user_id: {} --> BP optimize success...'.format(wear_user_id))
            bp_server_log.logger.info("优化成功...")
        except Exception as e:
            bp_server_log.logger.error('wear_user_id: {} 优化失败...{}'.format(wear_user_id, e))
            # 发送消息到钉钉群
            send_ding_message('wear_user_id: {} --> BP optimize error: {}...'.format(wear_user_id, e))

        # main_train(wear_user_id, opt_status)


#####################################
class Greeter(anbpOptimizer_pb2_grpc.OptimizerGreeterServicer):
    
    def ANBP(self, request, context):
        try:
            # 用户 wear_user_id
            wear_user_id = request.wearUserId
            # 优化类型: '0', 代表首次优化, '1', 代表新增数据的优化, '2',代表删除之前的提交数据,需重新优化
            opt_status = request.optStatus
            bp_server_log.logger.info("wearUserId: {} optStatus: {}...".format(wear_user_id, opt_status))

            # 开启异步
            t1 = threading.Thread(target=optimizer, args=(wear_user_id, opt_status))
            t1.start()
            # t1.join()

            # 发送消息到钉钉群
            return anbpOptimizer_pb2.OptimizerBPReply(status=ANBP_RES_SUCCEED)
        except Exception as e:
            bp_server_log.logger.error('wear_user_id: {} 血压优化 error: {}'.format(wear_user_id, e))
            # 发送消息到钉钉群
            # send_ding_message('wear_user_id: {} --> BP optimize error: {}'.format(wear_user_id, e))
            return anbpOptimizer_pb2.OptimizerBPReply(status=ANBP_RES_FAILD)

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    anbpOptimizer_pb2_grpc.add_OptimizerGreeterServicer_to_server(Greeter(), server)
    server.add_insecure_port('[::]:{}'.format(PORT))
    server.start()
    bp_server_log.logger.info("服务启动成功...")
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:   
        server.stop(0)
        bp_server_log.logger.info("服务关闭...")


if __name__ == '__main__':
    server()