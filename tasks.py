from celery import Celery
import redis
import json
import pymongo
import time
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import json_util
import heapq
from python_json_config import ConfigBuilder

config = ConfigBuilder().parse_config('../../config.json')

HOST = config.server.host
PORT = config.server.port
DB = 0

app = Celery(
    'tasks', backend='redis://' + HOST + ':' + PORT.celery + '/' + str(DB),
    broker='redis://' + HOST + ':' + PORT.redis + '/' + str(DB)
)
conn = MongoClient(HOST + ':' + PORT.mongo)
db = conn.sensor
init = {"cnt": 0, "heap": [], "lookup": [], "max_length": config.fileset.summary_count, "make_heap": -1, "insert": -1}

POOL = redis.ConnectionPool(host=HOST, port=PORT.redis, db=DB)
try:
    rconn = redis.Redis(connection_pool=POOL)
    rconn.ping()
    print("Redis is connected!")
except redis.ConnectionError:
    print("Redis connection error!")

# celery 연결 테스트
@app.task
def ping_celery():
    result = 'connected'
    return str(json.dumps(result, default=json_util.default))

# 데이터 삽입 / mongodb-sensor-data
@app.task
def insert(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data": value}
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])
    db.data.insert(doc)

# 우선순위 큐를 사용해 heap구조로 저장 / redis['센서 명']
@app.task
def make_heap(doc):
    doc = json.loads(doc)
    data = json.loads(rconn.get(doc["dev_name"]))
    data['lookup'].append(doc['data'])
    heapq.heappush(data['heap'], [doc['data'], data['cnt']])
    data['cnt'] += 1
    data['make_heap'] = data['cnt']
    data['insert'] = data['cnt']
    rconn.set(doc["dev_name"], json.dumps(data))

    if data['cnt'] % data['max_length'] == 0:
        rconn.set(doc["dev_name"] + 'summary', json.dumps(data))

        summary_data = [doc, data['heap'], data['lookup']]
        app.send_task('tasks.insert_summary', args=[json.dumps(summary_data)])

        init["cnt"] = data['cnt']
        init["make_heap"] = data['cnt']
        rconn.set(doc["dev_name"], json.dumps(init))

# 누적 데이터 요약정보 삽입 / mongodb-sensor-summary
@app.task
def insert_summary(summary_data):
    doc, heap, lookup = json.loads(summary_data)
    del doc['data']
    doc['max'] = heapq.nlargest(1, heap)[0][0]
    doc['min'] = heapq.nsmallest(1, heap)[0][0]
    doc['avg'] = sum(lookup) / len(lookup)
    db.summary.insert(doc)
    print('insert_summary')

# 센서 데이터 출력 / mongodb-sensor-data
@app.task
def maxmin(dname, flag):
    if flag == True:
        rname = dname + 'summary'
        tmp = json.loads(rconn.get(rname))
    else:
        tmp = json.loads(rconn.get(dname))
    heap = tmp["heap"]
    length = len(tmp["lookup"])

    avg = sum(tmp["lookup"]) / length
    res = [heap, heapq.nlargest(1, heap)[0][0], heapq.nsmallest(1, heap)[0][0], avg]
    return json.dumps(res)

# summary 출력 / mongodb-sensor-summary
@app.task
def summary():
    doc_list = list(db.summary.find({}, {'_id': 0}))
    return str(json.dumps(doc_list, default=json_util.default))

# DB에 등록되어있는 센서 목록 출력
@app.task
def get_sname():
    li = list(db.device.find({}, {'dev_name': 1}))
    names = []
    for sensor in li:
        names.append(sensor['dev_name'])
    return str(json.dumps(names, default=json_util.default))

# data 검색 / mongodb-sensor-data
@app.task
def find(tf):
    if tf == None:
        doc_list = list(db.data.find({}, {'_id': 0}))
    else:
        doc_list = list(db.data.find({'error': tf}, {'_id': 0}))
    return str(json.dumps(doc_list, default=json_util.default))

# data 분석 / mongodb-sensor-data
@app.task
def analy(dname, n):
    # 복합 Query를 위한 Pipeline 구성
    Pipeline = list()
    Pipeline.append({'$match': {'dev_name': dname, 'error': False}})
    Pipeline.append({'$sort': {'date': -1}})
    Pipeline.append({'$limit': int(n)})
    Pipeline.append(
        {'$group': {'_id': 'data_analy', 'avg': {'$avg': '$data'}, 'max': {'$max': '$data'}, 'min': {'$min': '$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

# 기준시각 이후 data 분석 / mongodb-sensor-data
@app.task
def analy_d(date):
    Pipeline = list()
    Pipeline.append({'$match': {'date': {'$gte': date}}})
    Pipeline.append(
        {'$group': {'_id': '$dev_name', 'avg': {'$avg': '$data'}, 'max': {'$max': '$data'}, 'min': {'$min': '$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

# 모든 collection 삭제
@app.task
def delete():
    db.data.drop()
    db.summary.drop()


if __name__ == '__main__':
    _main()
