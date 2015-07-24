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
from hbase.ttypes import TScan,ColumnDescriptor,AlreadyExists,Mutation,BatchMutation
import hbase.ttypes as htt
import struct
import logging

host, port = "10.2.25.110", 9090    # thrift server start at 110

tbl = "tr_bay_no_url"
epoch = datetime(1970,1,1)

logging.basicConfig()
logger = logging.getLogger()

buf_max_size = 10000


def byte_cid(cid):
    high = (cid>>8) & 0xff
    # low = (-128) + (255-cid&0xff)

    r = cid%256
    low = -(r-127) if r>127 else r

    return struct.pack('bb',high,low)

def unbyte_cid(bstr):
    r = struct.unpack("bb", bstr)

    low = 127-r[1] if r[1]<0  else r[1]
    rs = "%s%s" % (chr(r[0]), chr(low))

    return struct.unpack(">H",rs)[0]

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

def _stat_one_bay(bayid,stime,etime):
    tseries = daily(stime,etime)

    tseries1 = []
    for item in tseries:
        tseries1.append((item-epoch).total_seconds()*1000.0)

    count = []
    for i in xrange(1,len(tseries1)):
        scan = htt.TScan()
        scan.columns = ['cf:']
        scan.caching = 110
        scan.startRow = struct.pack(">HQ", int(bayid), tseries1[i-1])
        scan.stopRow = struct.pack(">HQ", int(bayid), tseries1[i])

        scanner = client.scannerOpenWithScan(tbl, scan, None)

        result = client.scannerGetList(scanner, 100000)
        cnt = 0
        for item in result:
            cnt += 1

        count.append(cnt)

    return count

    # rowkey = struct.pack(">HQ10s", bayid, passtime, numb)

def stat_volume(stime,etime):
    tgsinfo = read_tgs_info()

    # from multiprocessing.dummy import Pool as ThreadPool
    from multiprocessing.pool import Pool

    pool = Pool()
    volume = [pool.apply_async(stat_tgs_volume,args=(stime,etime,int(cid))) for cid in tgsinfo.keys()]
    pool.close()

    print 'waiting to join....'
    pool.join()

    print 'start to writing to file...'

    volume0 = []
    for i,elem in enumerate(volume):
        volume0.append((tgsinfo.keys()[i], elem.get()))
    volume0.sort(key=lambda x:x[1], reverse=True)

    total = 0
    with open(os.path.join(root_dir, "result", "volume.txt"),"w") as f:
        for i,elem in enumerate(volume0):
            # cid = tgsinfo.keys()[i]
            # vol = elem.get()
            total += elem[1]

            line = "%5s,%s: %d\n" % (elem[0], tgsinfo[elem[0]]['kkmc'], elem[1])
            f.write(line)

    print 'totally %d records.' % (total)

def _query_single_bay(client,cid,numb,ptype,stime,etime):
    result = []

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = ""
    #
    scan.startRow = struct.pack(">HQ", int(cid), (stime-epoch).total_seconds()*1000)
    scan.stopRow = struct.pack(">HQ", int(cid), (etime-epoch).total_seconds()*1000)

    scanner = client.scannerOpenWithScan(tbl, scan, None)
    dataset = client.scannerGetList(scanner, 100000)

    cnt = 0
    for cont in dataset:
        cont_str = cont.columns['cf:'].value[:54]

        v_numb = struct.unpack(">10s", cont_str[11:21])[0]
        idx = v_numb.find("\x00")
        v_numb1 = v_numb[:idx]

        v_type = struct.unpack(">B", cont_str[35:36])[0]
        # print "%s,%s" % (v_numb1,v_type)
        # print len(v_numb1),len(numb)

        if v_numb1!=numb: # or v_type!=ptype:
            continue

        row = {}
        row['passtime'] = struct.unpack(">Q", cont_str[1:9])[0]
        row['cid'] = struct.unpack(">H", cont_str[29:31])[0]
        row['drivedir'] = struct.unpack(">B", cont_str[37:38])[0]
        row['driveway'] = struct.unpack(">I", cont_str[23:27])[0]
        row['vtype'] = struct.unpack(">B", cont_str[35:36])[0]

        print row

        result.append(row)
        cnt += 1

    if cnt > 0:
        print cid, '-->', cnt

    return result

def query_vehicle_trajetory(client,numb,ptype,stime,etime):
    tgsinfo = read_tgs_info()

    # from multiprocessing.dummy import Pool as ThreadPool
    from multiprocessing import Pool

    pool = Pool()
    result = []
    for cid in tgsinfo.keys():
        result.append(pool.apply(_query_single_bay, (cid,numb,ptype,stime,etime)))

    pool.close()
    pool.join()

    traj = []
    for item in result:
        for i in item:
            traj.append(i)

    print 'totally %d records.' % (len(traj))

    return traj

def query_vehicle_trajetory1(numb,ptype,stime,etime):
    tgsinfo = read_tgs_info()

    s = time.time()

    result = []
    # for cid,info in tgsinfo.iteritems():

    begtime = (stime-epoch).total_seconds()*1000
    endtime = (etime-epoch).total_seconds()*1000

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    # scan.filterString = "RowFilter(=,'substring:%s')" % (numb)
    # scan.startRow = struct.pack(">HQ", int(cid), begtime)
    # scan.stopRow = struct.pack(">HQ", int(cid), endtime)

    scanner = client.scannerOpenWithScan(tbl, scan, None)
    dataset = client.scannerGetList(scanner, 100000)

    cnt = 0
    for cont in dataset:
        # cont_str = cont.columns['cf:'].value[:54]

        # NUMB = cont.row[10:]

        # v_numb = struct.unpack(">10s", cont_str[11:21])[0]
        # idx = v_numb.find("\x00")
        # v_numb1 = v_numb[:idx]

        # v_type = struct.unpack(">B", cont_str[35:36])[0]
        # print "%s,%s" % (v_numb1,v_type)
        # print len(v_numb1),len(numb)

        # if v_numb1!=numb: # or v_type!=ptype:
            # continue

        row = {}
        # row['passtime'] = struct.unpack(">Q", cont_str[1:9])[0]
        # row['cid'] = struct.unpack(">H", cont_str[29:31])[0]
        # row['drivedir'] = struct.unpack(">B", cont_str[37:38])[0]
        # row['driveway'] = struct.unpack(">I", cont_str[23:27])[0]
        # row['vtype'] = struct.unpack(">B", cont_str[35:36])[0]

        row['passtime'] = struct.unpack(">Q", cont.row[2:10])[0]
        row['cid'] = struct.unpack(">H", cont.row[:2])[0]

        # print row

        result.append(row)
        cnt += 1

    # if cnt > 0:
        # print cid, '-->', cnt
        #

    e = time.time()

    print 'totally %d records in %.3f secs.' % (len(result), (e-s))

    return result

def query_vehicle(numb,stime,etime):
    (client,trpt) = get_client(host,port)
    tbl = "tr_plate61"

    numb_str = "%s" % (numb)

    # epoch = datetime(1970,1,1)
    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    trpt.open()

    scan = TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=,'binaryprefix:%s') AND KeyOnlyFilter()" % (numb)
    scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">10sQ", numb_str, begtime)
    scan.stopRow = struct.pack(">10sQ", numb_str, endtime)

    scanner = client.scannerOpenWithScan(tbl, scan, None)
    result = client.scannerGetList(scanner, 100000)

    tgsids = []
    for elem in result:
        # tgsid = struct.unpack(">H", elem.row[20:])[0]
        tgsid = unbyte_cid(elem.row[18:20])
        passtime = struct.unpack(">Q", elem.row[10:18])[0]

        tgsids.append({'passtime':datetime.fromtimestamp(passtime/1000.0), 'cid':tgsid})

    tgsids.sort(key=lambda x:x['passtime'], reverse=False)

    print "%d records fetched between [%s,%s)." % (len(tgsids),
                                                 stime.strftime("%Y-%m-%d %H:%M:%S"),
                                                 etime.strftime("%Y-%m-%d %H:%M:%S"))

    trpt.close()

    return tgsids

def stat_tgs_volume(stime,etime, cid):
    tbl = 'tr_bay61'

    (client,trpt) = get_client(host,port)

    trpt.open()

    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    b_cid = byte_cid(cid)

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">2sQ", b_cid, begtime)
    scan.stopRow = struct.pack(">2sQ", b_cid, endtime)

    scanner = client.scannerOpenWithScan(tbl, scan, None)

    # result = []
    count = 0
    while 1:
        dataset = client.scannerGetList(scanner, buf_max_size)
        # print '%d records fetched.' % (len(dataset))

        for elem in dataset:
            count += 1
            # print bytes(elem.row)
            # print unbyte_cid(elem.row[10:12])
            # print
            # result.append({
            #     'passtime': datetime.fromtimestamp(struct.unpack('>Q',elem.row[:8])[0]/1000.0).strftime('%Y-%m-%d %H:%M:%S'),
            #     'cid': unbyte_cid(elem.row[18:20]),
            # })

        if len(dataset) < buf_max_size:
            break

    trpt.close()

    # result.sort(key=lambda x: x['passtime'],reverse=False)

    print '%d: %d records fetched.' % (cid,count)

    return count

def query_traj(stime,etime, numb):
    tbl = 'tr_time_plate'

    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">Q10s", begtime, numb)
    scan.stopRow = struct.pack(">Q10s", endtime, numb)

    scanner = client.scannerOpenWithScan(tbl, scan, None)

    result = []
    while 1:
        dataset = client.scannerGetList(scanner, buf_max_size)

        for elem in dataset:
            result.append({
                'passtime': datetime.fromtimestamp(struct.unpack('>Q',elem.row[:8])[0]/1000.0).strftime('%Y-%m-%d %H:%M:%S'),
                'cid': unbyte_cid(elem.row[18:20]),
            })

        if len(dataset) < buf_max_size:
            break

    result.sort(key=lambda x: x['passtime'],reverse=False)

    print 'traj: %d records fetched.' % (len(result))

    return result




if __name__ == '__main__':
    # host, port = "10.2.15.3", 9090

    begtime = datetime(2015,6,1)
    endtime = datetime(2015,6,2)

    (client,trpt) = get_client(host,port)

    trpt.open()

    print client.getTableNames()
    # print tbls


    stat_volume(begtime,endtime)

    # cid = 151 if len(sys.argv)==1 else int(sys.argv[1])
    # stat_tgs_volume(begtime,endtime, cid)

    # tseries = daily(begtime,endtime)
    # print tseries

    # count = _stat_one_bay("70",begtime,endtime)
    # print count, ":", sum(count)

    # numb = u"新R00281".encode("utf-8")
    # res = match_vehicle(client,numb)
    # for row in res:
    #     print row

    # numb = u"鄂A78B07".encode("gbk")
    # res = query_vehicle(numb,begtime,endtime)
    # with open("traj_%s.txt" % (numb), "w") as f:
    #     for elem in res:
    #         line = "%s, %s\n" % (elem['passtime'].strftime("%Y-%m-%d %H:%M:%S"), elem['cid'])
    #         f.write(line)


    # numb = u"鄂AF8R13".encode("gbk")
    # trajetory = query_vehicle_trajetory1(numb,
    #                                     "2",
    #                                     begtime,
    #                                     begtime+timedelta(days=1))

    # trajetory.sort(key=lambda x:x['passtime'], reverse=False)
    # for item in trajetory:
    #     item['passtime'] = datetime.fromtimestamp(item['passtime']/1000.0)

    # for elem in trajetory:
    #     print elem

    # with open(os.path.join(root_dir,"result","%s.txt" % (numb)), "w") as f:
    #     for row in trajetory:
    #         line = "%s, %s, %d, %s, %d\n" % (row['passtime'].strftime("%Y-%m-%d %H:%M:%S %f"),
    #                                      row['vtype'],
    #                                      row['cid'],
    #                                      row['drivedir'],
    #                                      row['driveway'])
    #         f.write(line)

    # result = {}
    # tgsinfo = read_tgs_info()
    # for cid in tgsinfo.keys():
    #     count = _stat_one_bay(cid,begtime,endtime)
    #     result[cid] = count

    # with open(os.path.join(root_dir,"result","volume.txt"),"w") as f:
    #     for cid,val in result.iteritems():
    #         line = "%s:%s\n" % (cid, ','.join(map(str,val)))
    #         f.write(line)

    trpt.close()
