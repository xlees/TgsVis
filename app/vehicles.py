#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')
sys.path.insert(0,'./gen-py')

from hbase import Hbase
# from hbase.ttypes import ColumnDescriptor,AlreadyExists,Mutation,BatchMutation
import hbase.ttypes as htt
import struct

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def get_client(host,port):
    try:
        # Make socket
        transport = TSocket.TSocket(host, port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # srv2 = Demo2.Client(TMultiplexedProtocol(protocol,"Demo2"))
        # srv1 = Dai.Client(TMultiplexedProtocol(protocol,"Dai"))

        # Create a client to use the protocol encoder
        client = Hbase.Client(protocol)

    except Thrift.TException, tx:
        print "tx='%s'" % (tx.message)
        print type(tx)

    return (client, transport)

def parse_content(cont_str):
    row = {}

    row['VERSION'] = struct.unpack(">B", cont_str[:1])[0]

    row['TIMESTAMP'] = struct.unpack(">Q", cont_str[1:9])[0]
    row['ORDER_NUMBER'] = struct.unpack(">H", cont_str[9:11])[0]
    row['CAR_PLATE_NUMBER'] = struct.unpack(">10s", cont_str[11:21])[0]
    row['SPEED'] = struct.unpack(">H", cont_str[21:23])[0]
    row['LANE_ID'] = struct.unpack(">I", cont_str[23:27])[0]
    row['CAMERA_LOCATION'] = struct.unpack(">H", cont_str[27:29])[0]
    row['BAY_ID'] = struct.unpack(">H", cont_str[29:31])[0]

    row['CAMERA_ORIENTATION'] = struct.unpack(">B", cont_str[31:32])[0]
    row['CAR_BRAND'] = struct.unpack(">B", cont_str[32:33])[0]
    row['CAR_COLOR'] = struct.unpack(">B", cont_str[33:34])[0]
    row['CAR_PLATE_COLOR'] = struct.unpack(">B", cont_str[34:35])[0]
    row['CAR_PLATE_TYPE'] = struct.unpack(">B", cont_str[35:36])[0]
    row['CAR_STATUS'] = struct.unpack(">B", cont_str[36:37])[0]
    row['TRAVEL_ORIENTATION'] = struct.unpack(">B", cont_str[37:38])[0]

    row['PLATE_COORDINATES'] = struct.unpack(">Q", cont_str[38:46])[0]
    row['DRIVER_COORDINATES'] = struct.unpack(">Q", cont_str[46:54])[0]

    row['IMAGE_URLS'] = struct.unpack(">%ds" % (len(cont_str[54:])), cont_str[54:])[0]

    return row



if __name__ == '__main__':
    host, port = "10.2.25.115", 9090

    (client,trpt) = get_client(host,port)

    trpt.open()

    res = client.getStandardTime()
    print 'res=',res

    # for i in xrange(1,8):
    #     print client.statVehicles("2015,06,0%d" % (i),"y-m-d-h-30m",True)

    print client.statVehicles("2015,23","y-w-D",False)

    # loc = client.statLocation("2015,06,03|y-m-d-h-30m","中国,湖北|z-p-c","")
    # print loc

    trpt.close()
