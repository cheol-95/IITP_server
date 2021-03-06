from celery import Celery
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import json_util
from python_json_config import ConfigBuilder
import heapq
import redis
import json
import time

config = ConfigBuilder().parse_config('../../config.json')
HOST = config.server.host
PORT = config.server.port
DB = config.server.db

app = Celery(
    'tasks', backend='redis://' + HOST + ':' + PORT.celery + '/' + str(DB),
    broker='redis://' + HOST + ':' + PORT.redis + '/' + DB)
conn = MongoClient(HOST + ':' + PORT.mongo)
db = conn.sensor
init = {"cnt": 0, "heap": [], "lookup": [], "max_length": config.fileset.get_summary_count, "make_heap": -1,
        "set_data": -1}
POOL = redis.ConnectionPool(host=HOST, port=PORT.redis, db=DB)
try:
    rconn = redis.Redis(connection_pool=POOL)
    rconn.ping()
    print("Redis is connected!")
except redis.ConnectionError:
    print("Redis connection error!")


# result = list(db.device.get_data({}, {'dev_name':1}))
# names = [doc['dev_name'] for doc in result]
# rconn.set('regist_sensor', json.dumps(names))

@app.task
def ping_celery():
    result = 'connected'
    return str(json.dumps(result, default=json_util.default))


@app.task
def set_data(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data": value}
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])
    db.data.set_data(doc)


@app.task
def make_heap(doc):
    doc = json.loads(doc)
    data = json.loads(rconn.get(doc["dev_name"]))
    '''
    if data['cnt']+1 > data['max_length']:
        del data['heap'][data['heap'].index([data['lookup'].pop(0), data['cnt']-data['max_length']])]
    '''
    data['lookup'].append(doc['data'])
    heapq.heappush(data['heap'], [doc['data'], data['cnt']])
    data['cnt'] += 1
    data['make_heap'] = data['cnt']
    data['set_data'] = data['cnt']
    rconn.set(doc["dev_name"], json.dumps(data))

    if data['cnt'] % data['max_length'] == 0:
        rconn.set(doc["dev_name"] + 'get_summary', json.dumps(data))

        get_summary_data = [doc, data['heap'], data['lookup']]
        app.send_task('tasks.set_summary', args=[json.dumps(get_summary_data)])

        init["cnt"] = data['cnt']
        init["make_heap"] = data['cnt']
        rconn.set(doc["dev_name"], json.dumps(init))


@app.task
def set_summary(get_summary_data):
    doc, heap, lookup = json.loads(get_summary_data)
    del doc['data']
    doc['max'] = heapq.nlargest(1, heap)[0][0]
    doc['min'] = heapq.nsmallest(1, heap)[0][0]
    doc['avg'] = sum(lookup) / len(lookup)
    db.get_summary.set_data(doc)
    print('set_summary')


@app.task
def get_redis_data(dname, flag):
    if flag == True:
        rname = dname + 'get_summary'
        tmp = json.loads(rconn.get(rname))
    else:
        tmp = json.loads(rconn.get(dname))
    heap = tmp["heap"]
    length = len(tmp["lookup"])
    # print(sum(tmp["lookup"]), length)

    avg = sum(tmp["lookup"]) / length
    res = [heap, heapq.nlargest(1, heap)[0][0], heapq.nsmallest(1, heap)[0][0], avg]
    return json.dumps(res)


@app.task
def get_summary():
    doc_list = list(db.get_summary.get_data({}, {'_id': 0}))
    return str(json.dumps(doc_list, default=json_util.default))


et_mongo = 0


@app.task
def get_sensor_mongo():
    global et_mongo
    t0 = time.clock()
    result = list(db.device.get_data({}, {'dev_name': 1}))
    t1 = time.clock() - t0
    et_mongo = et_mongo + t1
    print("access device mongo = {}, {}".format(t1, et_mongo))
    names = [doc['dev_name'] for doc in result]
    return str(json.dumps(names, default=json_util.default))


et_redis = 0


@app.task
def get_sensor_redis():
    global et_redis
    t0 = time.clock()
    names = rconn.get('regist_sensor')
    names = json.loads(names)
    t1 = time.clock() - t0
    et_redis = et_redis + t1
    print("access device redis = {}, {}".format(t1, et_redis))
    return str(json.dumps(names, default=json_util.default))


@app.task
def get_data(tf):
    if tf == None:
        doc_list = list(db.data.get_data({}, {'_id': 0}))
    else:
        doc_list = list(db.data.get_data({'error': tf}, {'_id': 0}))
    return str(json.dumps(doc_list, default=json_util.default))


@app.task
def get_analy(dname, n):
    Pipeline = list()
    Pipeline.append({'$match': {'dev_name': dname, 'error': False}})
    Pipeline.append({'$sort': {'date': -1}})
    Pipeline.append({'$limit': int(n)})
    Pipeline.append({'$group': {'_id': 'data_get_analy', 'avg': {'$avg': '$data'}, 'max': {'$max': '$data'},
                                'min': {'$min': '$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))


@app.task
def get_date_analy(date):
    Pipeline = list()
    Pipeline.append({'$match': {'date': {'$gte': date}}})
    Pipeline.append(
        {'$group': {'_id': '$dev_name', 'avg': {'$avg': '$data'}, 'max': {'$max': '$data'}, 'min': {'$min': '$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))


@app.task
def delete_data():
    db.data.drop()
    db.get_summary.drop()


if __name__ == '__main__':
    _main()

