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

HOST = '211.253.236.72'
PORT = 6379
DB=0

app = Celery('tasks', backend='redis://' + HOST+ ':' + str(PORT) +'/' + str(DB), broker='redis://' + HOST +':' + str(PORT) +'/' + str(DB))
conn = MongoClient(HOST+':27017')
db = conn.sensor

rconn = None
init = None
POOL = None

def _main():
    POOL = redis.ConnectionPool(host=HOST, port=PORT, db=DB)
    try:
        rconn = redis.Redis(connection_pool=POOL)
        rconn.ping()
        print ("Redis is connected!")
    except redis.ConnectionError:
       print ("Redis connection error!")

# 데이터 삽입 / mongodb-sensor-data
@app.task
def insert(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data":value}
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])
    db.data.insert(doc)

# 우선순위 큐를 사용해 min, max, avg값 획득
@app.task
def make_heap(doc):
    doc = json.loads(doc)
    init = {"cnt":0, "heap":[], "lookup":[], "max_length":20}
    rconn = redis.Redis(connection_pool=POOL)
    data = rconn.get(doc["dev_name"])

    if data == None:
        rconn.set(doc["dev_name"], json.dumps(init))
        data = rconn.get(doc["dev_name"])
    data = json.loads(data)

    if data['cnt'] > data['max_length']-1:
        del data['heap'][data['heap'].index([data['lookup'].pop(0), data['cnt']-data['max_length']])]

    data['lookup'].append(doc['data'])
    heapq.heappush(data['heap'], [doc['data'], data['cnt']])
    data['cnt'] += 1
    rconn.set(doc["dev_name"], json.dumps(data))
    if data['cnt']%data['max_length'] == 0:
        summary_data = [doc, data['heap'], data['lookup']]
        app.send_task('tasks.insert_summary', args=[json.dumps(summary_data)])
        time.sleep(3)
        rconn.set(doc["dev_name"], json.dumps(init))

# 평균 데이터 삽입 / mongodb-sensor-summary
@app.task
def insert_summary(inner_data):
    doc, heap, lookup = json.loads(inner_data)
    del doc['data']
    doc['max'] = heapq.nlargest(1,heap)[0][0]
    doc['min'] = heapq.nsmallest(1,heap)[0][0]
    doc['avg'] = sum(lookup)/len(lookup)
    db.summary.insert(doc)

# 데이터 출력 / mongodb-sensor-data
@app.task
def maxmin(dname):
    rconn = redis.Redis(connection_pool=POOL)
    tmp = json.loads(rconn.get(dname))
    heap = tmp["heap"]
    length = len(tmp["lookup"])
    avg = sum(tmp["lookup"])/length
    '''
    if length == 0:
        return json.dumps(None)
        avg = 0
    else:
        avg = sum(tmp["lookup"])/length
        '''
    res = [heap, heapq.nlargest(1,heap)[0][0], heapq.nsmallest(1,heap)[0][0], avg]
    return json.dumps(res)

# summary 출력 / mongodb-sensor-summary
@app.task
def summary():
    doc_list = list(db.summary.find( {}, {'_id':0}))
    return str(json.dumps(doc_list, default=json_util.default))


# data 검색 / mongodb-sensor-data
@app.task
def find(tf):
    if tf == None:
        doc_list = list(db.data.find( {},{'_id':0}))
    else:
        doc_list = list(db.data.find({ 'error':tf },{ '_id':0 }))
    return str(json.dumps(doc_list, default=json_util.default))

# data 분석 / mongodb-sensor-data
@app.task
def analy(dname, n):
    # 복합 Query를 위한 Pipeline 구성
    Pipeline = list()
    Pipeline.append({'$match' : {'dev_name':dname, 'error':False}})
    Pipeline.append({'$sort' : {'date':-1}})
    Pipeline.append({'$limit' : int(n)})
    Pipeline.append({'$group' : {'_id' : 'data_analy', 'avg' : {'$avg':'$data'}, 'max' : {'$max':'$data'}, 'min' : {'$min':'$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

# 기준시각 이후 data 분석 / mongodb-sensor-data
@app.task
def analy_d(date):
    Pipeline = list()
    Pipeline.append({'$match' : {'date':{'$gte':date}}})
    Pipeline.append({'$group' : {'_id' : '$dev_name', 'avg' : {'$avg':'$data'}, 'max' : {'$max':'$data'}, 'min' : {'$min':'$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

# 모든 Collections 삭제
@app.task
def delete():
    db.data.drop()
    db.summary.drop()


if __name__ == '__main__':
    _main()