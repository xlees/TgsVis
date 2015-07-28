#coding: utf-8

from app import app
from flask import render_template,url_for,jsonify,request
import pandas as pd
import json

from app.helper import EvilTransform
from test.test_asso import *
from shared import *
from analysis import load_od_data
from dateutil.parser import parse

cols = ['KKID','KKMC','CLOUD_ID','X','Y']
tgsinfo = read_tgs_info()


@app.route('/request-od-data')
def query_od_data():
    index = _calc_dtime_index(request.args.get("datetime").strip())
    o_or_d = request.args.get("od").strip()

    od = load_od_data(index,o_or_d)

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

def _calc_dtime_index(dtime):
    granula = 10*60

    dtime = parse(dtime)   # drop seconds
    dtime = dtime.replace(second=0,microsecond=0)

    begtime = dtime.replace(hour=0,minute=0)
    index = (dtime-begtime).total_seconds() / granula

    return int(index)

@app.route('/calc-time-index')
def calc_time_index():
    index = _calc_dtime_index(request.args.get("datetime"))

    return jsonify({'index':int(index)})

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

@app.route('/')
@app.route('/index')
def index():
    context = {

    }

    return render_template("index.html")

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

    volume = read_tgs_volume("data/volume.txt")

    adj = Counter()
    with open("filterd_pairs.txt","r") as f:
        for line in f.readlines():
            pair = tuple(line.split(":")[0].split(","))
            adj[pair] = float(line.split(":")[1].split(",")[0])

    # adj = read_adj("data/week_adj.txt")
    # assoc = calc_assoc(volume,adj)

    # result = assoc.most_common()
    # filtered = filter(lambda x: x[1]>0.1, result)

    print '%d edges...' % (len(adj))

    # standardize
    vol = volume.most_common()
    vol1 = [(item[0],float(item[1]-vol[-1][1]) / (vol[0][1]-vol[-1][1])) for item in vol]

    ret = {
        'status': 0,
        # 'data': [[item[0][0],item[0][1],item[1]] for item in filtered],
        'data': [[val[0][0],val[0][1],val[1]] for val in adj.items()],
        'volume': dict(vol1),
    }

    return jsonify(ret)

# @app.route('/get-adj1')
# def get_adj_tgs1():
#     from itertools import combinations
#     import random
#     import re

#     thresh = 80
#     p_dest = re.compile("\(\d+")
#     p = re.compile("\d+-\d+-\d+")

#     pairs = []
#     with open("adj.txt","r") as f:
#         lines = f.read().split("\n")
#         for line in lines:
#             try:
#                 dest = p_dest.match(line).group()[1:]
#             except AttributeError,e:
#                 print 'parse error'
#                 print line
#                 continue

#             for item in p.findall(line):
#                 source = item.split("-")
#                 if int(source[2]) < thresh:
#                     continue
#                 pairs.append((source[0], dest))
#         # avail.extend(f.read().split("\n"))

#     print 'totally %d pairs' % (len(pairs))

#     # comb = []
#     # for item in combinations(avail,2):
#     #     comb.append(item)

#     # print 'all ', len(comb), ' combinations.'

#     # choice = random.sample(comb, len(comb)/500)
#     # print 'select ', len(choice), ' pairs.'

#     return jsonify({'data':pairs})

@app.route('/query-tgs')
def query_tgs_info():
    cid = request.args.get('numb', '')

    ret = {}

    if cid == '':
        ret = {
            'status': -1,
            'msg': u'无卡口输入！',
        }
    else:
        tgs_info = pd.read_csv('tgsinfo.csv')[cols]
        res = tgs_info.apply(transform, axis=1).tolist()

        # read adjcency
        p_main = re.compile("\d+,")
        p_indegree = re.compile("\d+-\d+")
        fname = "data/week_adj.txt"
        upstream = Counter()
        with open(fname,"r") as f:
            for line in f.readlines():
                try:
                    main = p_main.search(line).group()[:-1]
                except AttributeError,e:
                    print e.args[0], ":", line
                    continue

                if main == cid:
                    indegree = p_indegree.findall(line)
                    for d in indegree:
                        item = d.split("-")
                        if item[0]=='0' or item[0]==main:
                            continue
                        upstream[item[0]] = int(item[1])

                    # break

        # print 'upstream:', upstream
        u0 = upstream.most_common(10)
        u1 = [(item[0], float(item[1]-u0[-1][1]) / (u0[0][1]-u0[-1][1])) for item in u0]
        u2 = [item if item[1]>0.5 else (item[0], 0.1) for item in u1]
        top10 = dict(u2)

        print 'u1: ', u2

        filtered = filter(lambda x: x[2]==cid, res)
        if len(filtered) == 0:
            ret['status'] = 1
            ret['msg'] = u'该卡口不存在！'
        else:
            ret['status'] = 0
            ret['data'] = {'main':filtered[0],'upstream':top10}

    return jsonify(ret,ensure_ascii=False)

@app.route('/load-tgs')
def load_tgs_info():

    tgs_info = pd.read_csv('tgsinfo.csv')[cols]
    # temp = tgs_info[cols].to_dict()

    res = tgs_info.apply(transform, axis=1).tolist()

    # res = list(tgs_info[cols].itertuples())
    # res1 = [dict(zip(cols,item[1:])) for item in res]

    print 'length of res:', len(res),'typeof : ', type(res)
    print res[:3]

    # load available kakou
    avail = []
    with open("avail.txt","r") as f:
        avail.extend(f.read().split("\n"))
    # print 'avail ',avail

    ret = {
        'data': res,
        'avail': avail,
        'status': 0,
    }

    return json.dumps(ret,ensure_ascii=False)
