#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
sys.path.append(r'../proto/')
import numpy as np
import pandas as pd
import grpc
import anbpOptimizer_pb2, anbpOptimizer_pb2_grpc

# IP = "192.168.100.195:50058"
# IP = "123.56.141.93:50059"


# 5ae46115 33f43701   73d3a978  d8877080  fc5cb908  3df13694  c5071592  8d4d2680 1486c491
# 本地服务ip
IP = "127.0.0.1:50058"
wear_user_id = "d9bNkAGS"
# wear_user_id = "7a2e8b44"
# wear_user_id = "7294140f"


def run():
    channel = grpc.insecure_channel(IP)
    stub = anbpOptimizer_pb2_grpc.OptimizerGreeterStub(channel=channel)
    response = stub.ANBP(anbpOptimizer_pb2.OptimizerBPRequest(wearUserId = wear_user_id, optStatus='0'))
    print("status: {} ...".format(response.status))


if __name__ == "__main__":

    run()