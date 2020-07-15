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
config = ConfigBuilder().parse_config('./config.json')

HOST = config.server.host
PORT = config.server.port
DB=0

app = Celery(
        'tasks', backend='redis://' + HOST+ ':' + str(PORT.celery) +'/' + str(DB),
        broker='redis://' + HOST +':' + str(PORT.redis) +'/' + str(DB)
        )

conn = MongoClient(HOST+':'+PORT.mongo)
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


@app.task
def insert(dname, value):
    doc = {"date": int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19]), "dev_name": dname, "data":value}
    app.send_task('tasks.make_heap', args=[json.dumps(doc)])
    db.data.insert(doc)

@app.task
def make_heap(doc):
    doc = json.loads(doc)
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
 

@app.task
def insert_summary(inner_data):
    doc, heap, lookup = json.loads(inner_data)
    del doc['data']
    doc['max'] = heapq.nlargest(1,heap)[0][0]
    doc['min'] = heapq.nsmallest(1,heap)[0][0]
    doc['avg'] = sum(lookup)/len(lookup)
    db.summary.insert(doc)


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

@app.task
def summary():
    doc_list = list(db.summary.find( {}, {'_id':0}))
    return str(json.dumps(doc_list, default=json_util.default))

@app.task
def get_sname():
    li = list(db.device.find( {}, {'dev_name':1} ))
    names = []
    for sensor in li:
        names.append(sensor['dev_name'])
    return str(json.dumps(names, default=json_util.default))

@app.task
def find(tf):
    if tf == None:
        doc_list = list(db.data.find( {},{'_id':0}))
    else:
        doc_list = list(db.data.find({ 'error':tf },{ '_id':0 }))
    return str(json.dumps(doc_list, default=json_util.default))

@app.task
def analy(dname, n):
    Pipeline = list()
    Pipeline.append({'$match' : {'dev_name':dname, 'error':False}})
    Pipeline.append({'$sort' : {'date':-1}})
    Pipeline.append({'$limit' : int(n)})
    Pipeline.append({'$group' : {'_id' : 'data_analy', 'avg' : {'$avg':'$data'}, 'max' : {'$max':'$data'}, 'min' : {'$min':'$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

@app.task
def analy_d(date):
    Pipeline = list()
    Pipeline.append({'$match' : {'date':{'$gte':date}}})
    Pipeline.append({'$group' : {'_id' : '$dev_name', 'avg' : {'$avg':'$data'}, 'max' : {'$max':'$data'}, 'min' : {'$min':'$data'}}})
    res = list(db.data.aggregate(Pipeline))
    return str(json.dumps(res, default=json_util.default))

@app.task
def delete():
    db.data.drop()
    db.summary.drop()


if __name__ == '__main__':
    _main()
