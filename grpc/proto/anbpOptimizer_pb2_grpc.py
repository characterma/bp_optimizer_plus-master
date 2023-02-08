# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import anbpOptimizer_pb2 as anbpOptimizer__pb2


class OptimizerGreeterStub(object):
    """创建服务
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ANBP = channel.unary_unary(
                '/pyserver.OptimizerGreeter/ANBP',
                request_serializer=anbpOptimizer__pb2.OptimizerBPRequest.SerializeToString,
                response_deserializer=anbpOptimizer__pb2.OptimizerBPReply.FromString,
                )


class OptimizerGreeterServicer(object):
    """创建服务
    """

    def ANBP(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_OptimizerGreeterServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ANBP': grpc.unary_unary_rpc_method_handler(
                    servicer.ANBP,
                    request_deserializer=anbpOptimizer__pb2.OptimizerBPRequest.FromString,
                    response_serializer=anbpOptimizer__pb2.OptimizerBPReply.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'pyserver.OptimizerGreeter', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class OptimizerGreeter(object):
    """创建服务
    """

    @staticmethod
    def ANBP(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pyserver.OptimizerGreeter/ANBP',
            anbpOptimizer__pb2.OptimizerBPRequest.SerializeToString,
            anbpOptimizer__pb2.OptimizerBPReply.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)