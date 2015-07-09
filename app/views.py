from app import app
from flask import render_template,url_for,jsonify,request
import pandas as pd
import json

from app.helper import EvilTransform


@app.route('/')
@app.route('/index')
def index():
    context = {

    }

    return render_template("index.html")

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
    from itertools import combinations
    import random
    import re

    thresh = 80
    p_dest = re.compile("\(\d+")
    p = re.compile("\d+-\d+-\d+")

    pairs = []
    with open("adj.txt","r") as f:
        lines = f.read().split("\n")
        for line in lines:
            try:
                dest = p_dest.match(line).group()[1:]
            except AttributeError,e:
                print 'parse error'
                print line
                continue

            for item in p.findall(line):
                source = item.split("-")
                if int(source[2]) < thresh:
                    continue
                pairs.append((source[0], dest))
        # avail.extend(f.read().split("\n"))

    print 'totally %d pairs' % (len(pairs))

    # comb = []
    # for item in combinations(avail,2):
    #     comb.append(item)

    # print 'all ', len(comb), ' combinations.'

    # choice = random.sample(comb, len(comb)/500)
    # print 'select ', len(choice), ' pairs.'

    return jsonify({'data':pairs})


@app.route('/load-tgs')
def load_tgs_info():
    cols = ['KKID','KKMC','CLOUD_ID','X','Y']

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
