#coding: utf-8

from app import app
from flask import render_template,url_for,jsonify,request,render_template_string
import pandas as pd
import json
import time
import math
import os

# from app.helper import EvilTransform
# from test.test_asso import *
import shared as shd
import analysis as aly
# from analysis import load_od_data
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import datetime

# cols = ['KKID','KKMC','CLOUD_ID','X','Y']
tgsinfo = shd.read_tgs_info()               # all tgs info
adj_out = aly.load_adj_info(False)
adj_in = aly.load_adj_info(True)


@app.route('/get-route-at')
def get_route_at():
    max_lines = 150

    path_len = request.args.get('path-length',2,type=int)

    print 'path length=',path_len

    fname = os.path.join(shd.root_dir,'data','od-routes', '%d.txt' % (path_len))
    if not os.path.exists(fname):
        print 'length of %d not exists.' % (path_len)
        return jsonify({'status':1, 'msg':u'路径长度有误！'.encode('utf-8')})

    routes = []
    cnt = 0
    with open(fname,'r') as f:
        for line in f.readlines():
            key,val = line[:-1].split(':')
            # routes[key] = int(val)
            routes.append((key.replace(",","-"), int(val)))

            cnt += 1
            if cnt > max_lines:
                break

    print '%d routes fetched.' % (len(routes))

    ret = {
        'status': 0,
        'data': routes,
    }

    return jsonify(ret)

@app.route('/get-day-travel-span')
def get_day_travel_span():
    now = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)

    begtime = request.args.get('begtime',(now+relativedelta(months=-1)).strftime("%Y-%m-%d %H:%M:%S"),type=str)
    endtime = request.args.get('endtime',now.strftime("%Y-%m-%d %H:%M:%S"),type=str)
    numb = request.args.get("numb",u"鄂AF8R13".encode("utf-8"), type=str).decode("utf-8").encode("gbk")
    ptype = request.args.get("ptype","02",type=str)

    print "numb: ",numb,type(numb)

    if not aly.check_vehicle_records(numb,ptype, parse(begtime),parse(endtime)):
        ret = {
            'status': 1,
            'msg': 'No passcar records.',
        }

        return jsonify(ret)

    tseries = shd.daily(parse(begtime), parse(endtime))

    # travel_freq = []
    freqs = {}
    spans = {}
    for i in xrange(0,len(tseries)-1):
        traj = aly.query_vehicle_trajetory(numb,ptype, tseries[i],tseries[i+1])
        tlist = aly.estimate_vehicle_freq(traj)
        span,count = aly.get_day_travel_span(tlist)

        timestamp = long(time.mktime(tseries[i].timetuple()))

        freqs[timestamp] = count
        spans[timestamp] = round(span,1)

    ret = {
        'status': 0,
        'tseries': tseries,
        'span': spans,
        'freq': freqs,
    }

    return jsonify(ret)

@app.route('/gps-to-bd')
def gps_to_bd():
    lng = request.args.get('lng',type=float)
    lat = request.args.get('lat',type=float)

    loc = shd.gps2baidu((lng,lat))
    if loc[0] is None and loc[1] is None:
        return jsonify({'status':1,'msg':'gps loc convert failure.'})

    return jsonify({'status':0,'data':loc})

@app.route('/request-tgs-info')
def response_tgs_info():
    # print tgsinfo['10588']

    locs = [(elem[1]['lng'], elem[1]['lat']) for elem in tgsinfo.items()]
    # print locs[0]

    # center = (0.0, 0.0)
    center = reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]), locs)
    c = map(lambda x: x/len(locs), center)

    result = {
        'status': 0,
        'data': tgsinfo,
        'center': c,
    }

    return json.dumps(result,ensure_ascii=False)

@app.route('/request-od-data')
def query_od_data():
    index = _calc_dtime_index(request.args.get("datetime").strip())
    o_or_d = request.args.get("od").strip()

    od = aly.load_od_data(index,o_or_d)

    ret = {
        'tgs': [elem[0] for elem in od],
        'count': [elem[1] for elem in od],
    }

    return jsonify(ret)

@app.route('/charts')
def od_charts():
    context = {

    }

    return render_template("od.html")

def _calc_dtime_index(date_time):
    granula = 10*60

    try:
        dtime = parse(date_time)   # drop seconds
    except:
        print 'datetime error: %s' % (date_time)
        return 0

    dtime = dtime.replace(second=0,microsecond=0)

    begtime = dtime.replace(hour=0,minute=0)
    index = (dtime-begtime).total_seconds() / granula

    return int(index)

@app.route('/calc-time-index')
def calc_time_index():
    index = _calc_dtime_index(request.args.get("datetime"))

    return jsonify({'index':int(index)})

@app.route('/load-map-control')
def load_map_control():
    res = render_template("in_out.html")

    return jsonify({'data':res})

@app.route('/get-o-data')
def get_odata():
    fname = os.path.join(root_dir,"result","o1.txt")

    dtime = []
    o = []
    with open(fname,"r") as f:
        for line in f.readlines():
            tmp = line[:-1].split(",")
            dtime.append(tmp[0])
            o.append(int(tmp[1]))

    return jsonify({'datetime':dtime, 'o':o})

@app.route('/get-d-data')
def get_ddata():
    fname = os.path.join(root_dir,"result","d1.txt")

    dtime = []
    d = []
    with open(fname,"r") as f:
        for line in f.readlines():
            tmp = line[:-1].split(",")
            dtime.append(tmp[0])
            d.append(int(tmp[1]))

    return jsonify({'datetime':dtime, 'd':d})

@app.route('/index')
def index():
    context = {

    }

    return render_template("index.html")

@app.route('/')
@app.route('/index1')
def index1():
    context = {

    }
    return render_template("index1.html")

@app.route('/add')
def add_numbers():
    a = request.args.get('a', 0, type=int)
    b = request.args.get('b', 0, type=int)

    print 'a=',a,' b=',b

    return jsonify(result=a + b)

def transform(row):
    from app.helper import EvilTransform

    loc = EvilTransform.transform(row['Y'],row['X'])

    # print type(row['KKMC']), row['KKMC']

    return (row['KKID'], row['KKMC'].decode("gbk").encode("utf8"), repr(int(row['CLOUD_ID'])), loc[0], loc[1])


@app.route('/query-vehicle')
def query_vehicle():
    ret = {
        'status':0,
        'data':[],
    }
    return jsonify(ret)

@app.route('/get-adj')
def get_adj_tgs():
    adj = {}
    for cid,info in adj_out.iteritems():
        # vol = 0
        n = 4 if len(info)>4 else len(info)
        for i in xrange(n):
            adj["%s,%s" % (cid,info[i][0])] = info[i][1]

    ret = {
        'status': 0,
        'data': adj,
    }

    return jsonify(ret)

@app.route('/query-tgs')
def query_tgs_info():
    cid = request.args.get('numb', '')
    up_down = request.args.get('dtype',0,type=int)      # default upstream

    print "up_down:",up_down

    ret = {}

    if cid == '':
        ret = {
            'status': -1,
            'msg': u'无卡口输入！',
        }
        return jsonify(ret,ensure_ascii=False)
    else:
        if not tgsinfo.has_key(cid):
            ret['status'] = 1
            ret['msg'] = u'该卡口不存在！'
            return jsonify(ret,ensure_ascii=False)

        # print 'upstream:', upstream
        # u0 = upstream.most_common()

        try:
            if up_down == 0:            # indegree
                u0 = adj_in[cid]
            else:
                u0 = adj_out[cid]
        except KeyError,e:
            print e
            u0 = []

        u1 = [(item[0], float(item[1]-u0[-1][1]) / (u0[0][1]-u0[-1][1])) for item in u0]
        u2 = [item for item in u1 if item[1]>0.4]
        top = dict(u2)

        print 'top edges: ', u2

        ret['status'] = 0
        ret['data'] = {'main':cid, 'upstream':top}

    return jsonify(ret,ensure_ascii=False)

# @app.route('/load-tgs')
# def load_tgs_info():

#     tgs_info = pd.read_csv('tgsinfo.csv')[cols]
#     # temp = tgs_info[cols].to_dict()

#     res = tgs_info.apply(transform, axis=1).tolist()

#     # res = list(tgs_info[cols].itertuples())
#     # res1 = [dict(zip(cols,item[1:])) for item in res]

#     print 'length of res:', len(res),'typeof : ', type(res)
#     print res[:3]

#     # load available kakou
#     avail = []
#     with open("avail.txt","r") as f:
#         avail.extend(f.read().split("\n"))
#     # print 'avail ',avail

#     ret = {
#         'data': res,
#         'avail': avail,
#         'status': 0,
#     }

#     return json.dumps(ret,ensure_ascii=False)
