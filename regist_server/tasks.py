from celery import Celery
import json
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import json_util
import time

HOST = '211.253.236.72'
PORT = 6379
DB = 0

app = Celery('tasks', backend='redis://' + HOST+ ':' + str(PORT) +'/' + str(DB), broker='redis://' + HOST +':' + str(PORT) +'/' + str(DB))
conn = MongoClient(HOST+':27017')
db = conn.sensor

@app.task
def regist(data):
    app.send_task('tasks.insert_key', args=[json.dumps(data)])
    _id  = db.device.insert(data)
    return str(_id)

@app.task
def insert_key(data):
    data = json.loads(data)
    kList = [key for key in data]
    for i in kList:
        doc = { 'key':i , 'sensor':data }
        db.keys.insert(doc)

@app.task
def kfind(value):
    kList = []
    if(value == None):
        li = list(db.keys.find({},{ 'key':1 }))
        for i in li:
            kList.append(i['key'])
        kList = list(set(kList))
    else:
        li = list(db.keys.find({ 'key':value },{ 'sensor':1 }))
        for i in li:
            tmp = i['sensor']
            kList.append(tmp)
    return str(json.dumps(kList, default=json_util.default))

@app.task
def find(key, value):
    if(key == None):
        li = list(db.device.find({}))
    else:
        if(key == '_id'):
            value = ObjectId(value)
        li = list(db.device.find({ key:value }))
    return str(json.dumps(li, default=json_util.default))

@app.task
def update(key, value, skey, svalue):
    db.device.update_one({ key:value },{'$set':{ skey: svalue }})
    result = db.device.find_one({ key:value })
    return json.dumps(result,default=json_util.default)

@app.task
def delete(key, value):
    if(key == None):
        db.device.drop()
        db.keys.drop()
    else:
        tmp = list(db.device.find({ key:value }))
        print(tmp)
        for i in tmp:
            app.send_task('tasks.delete_key', args=[json.dumps(i)])
        #result = db.device.remove({ key:value })

@app.task
def delete_key(data):
    data = json.loads(data)
    del data['_id']
    db.keys.remove({ 'sensor':data })
