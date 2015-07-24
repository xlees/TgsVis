#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.pardir))
os.chdir(root)
sys.path.insert(0,root)
# sys.path.insert(0, os.path.join(root_dir,"app","gen-py"))


from app.shared import *
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
# from hbase.ttypes import ColumnDescriptor,AlreadyExists,Mutation,BatchMutation
import hbase.ttypes as htt
import struct
from trajetory import byte_cid,unbyte_cid

tbl = "tr_plate"
epoch = datetime(1970,1,1)


def valid_record(fname):
    numb = u"鄂AF8R13".encode('gbk')

    traj = []
    with open(fname,"rb") as f:
        cnt = 0
        while 1:
            line = f.read(55)
            if len(line) == 0:
                break

            numb1 = struct.unpack('>10s',line[11:21])[0]
            cnt += 1

            if numb1.find(numb) < 0:
                continue

            passtime = datetime.fromtimestamp(struct.unpack('>Q',line[1:9])[0]/1000.0)

            traj.append(passtime.strftime("%Y-%m-%d %H:%M:%S"))

    print '%d records.' % (cnt)

    return traj


def tgs_match(tgsid,numb):
    epoch = datetime(1970,1,1)

    stime = long((datetime(2015,6,1)-epoch).total_seconds()*1000)
    etime = long((datetime(2015,6,1,9,0,0)-epoch).total_seconds()*1000)

    # begtime = long(time.mktime(stime.timetuple())*1000)
    # endtime = long(time.mktime(etime.timetuple())*1000)

    (client,trpt) = get_client(host,port)
    trpt.open()

    # pstr = "^%s.*%s" % (struct.pack('>H',tgsid), numb.encode('gbk'))
    # print 'pstr=',pstr

    numb1 = numb.encode('gbk')
    pstr = "^\x00\x80.*%s" % (numb.encode('gbk'))

    scan = htt.TScan()
    scan.columns = ['cf:']
    # scan.filterString = "RowFilter(=,'substring:%s')" % (numb1)
    scan.filterString = "RowFilter(=,'regexstring:^\x0a')" #% (pstr)
    # scan.filterString = "RowFilter(=,'binaryprefix:%s')" % (struct.pack('>H', tgsid))
    # scan.startRow = "%s%s" % (struct.pack('>H',tgsid), struct.pack('>Q',stime))
    # scan.stopRow = "%s%s" % (struct.pack('>H',tgsid), struct.pack('>Q',etime))
    # print 'start:', bytes(scan.startRow), 'stop:',bytes(scan.stopRow)

    scanner = client.scannerOpenWithScan("tr_bay_no_url", scan, None)

    # result = []
    matched = []
    size = 1000

    while 1:
        result = client.scannerGetList(scanner, size)

        for elem in result:
            key = struct.unpack('>H',elem.row[:2])[0]
            cont = struct.unpack('>H',elem.columns['cf:'].value[29:31])[0]

            print elem

            matched.append({'key':key,'cont':cont})

        if len(result) < size:
            break

    print '%d records fetched.' % (len(matched))

    trpt.close()

    return matched


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

def filter_vehicles(numb):
    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    scan.filterString = "RowFilter(=,'binaryprefix:%s')" % (numb)
    # scan.filterString = "PrefixFilter('%s')" % (numb)
    # scan.startRow = struct.pack(">HQ", int(bayid), tseries1[i-1])
    # scan.stopRow = struct.pack(">HQ", int(bayid), tseries1[i])

    scanner = client.scannerOpenWithScan(tbl, scan, None)

    result = client.scannerGetList(scanner, 100000)

    cnt = 0
    for cont in result:
        cnt += 1

    print '%s: %d records.' % (numb,cnt)

if __name__ == '__main__':
    host, port = "10.2.25.110", 9090    # thrift server start at 110
    # host, port = "10.2.15.3", 9090


    (client,trpt) = get_client(host,port)

    trpt.open()

    print client.getTableNames()

    # numb = "\x09\xE4\xBA\xACLL6661\x00\x00\x00\x00"
    # filter_vehicles(numb)

    cid = 255
    numb = u"鄂AF8R13"
    res = tgs_match(cid,numb)
    # for elem in res:
        # print elem

    # traj = valid_record(os.path.join(root_dir,"tmp","export_232.db"))
    # for elem in traj:
    #     print elem

    trpt.close()
