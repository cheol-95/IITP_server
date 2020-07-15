from tasks import insert, find, analy, analy_d, delete
import os
from datetime import datetime 
import json
import argparse

def parse_argument():
    usage = 'python test_task.py -f [device json file]\n       run with --help for argument descriptions'
    parser = argparse.ArgumentParser(usage = usage)
    parser.add_argument('-n', '--number', type=str, help='input analy count')
    parser.add_argument('-f', '--testfile', type=str, help='input sensor_json name')
    parser.add_argument('-c', '--command', type=str, help='input command')
    parser.add_argument('-s', '--status', type=str, help='input status')
    args=parser.parse_args()
    return args
args = parse_argument()

if(args.command == 'insert'):
    cnt = 0
    while cnt < 20:
        date = int(datetime.now().strftime('%Y%m%d%H%M%S%f')[:19])
        dname = args.testfile
        if dname == None:
            print('Error: Filename is None')
            exit()
        data = lm35_1.sensing()
        doc = { "date":date, "dev_name":dname, "data":data }
        print(doc)
        insert.delay(doc)
        cnt += 1
    print(f'insert data count : {cnt}')

def TF(s):
    if s == None:
        return None
    elif s.lower() == 'false':
        return False
    return True

if(args.command == 'find'):
    args.status = TF(args.status)
    result = find.delay(args.status)
    res = json.loads(result.get())
    print(f'total data count : {len(res)}')
    for i in res:
        print(i)

if(args.command == 'analy'):
    result = analy.delay(args.testfile, args.number)
    res = json.loads(result.get())
    for i in res:
        print(f'{args.testfile}의 최근 {args.number}개  최고:{i["max"]},  최저:{i["min"]},  평균:{i["avg"]}')

if(args.command == 'date'):
    date = int(args.number + (19 - len(args.number))*'0')
    result = analy_d.delay(date)
    res = json.loads(result.get())
    tmp = str(date)
    date_in = f'{tmp[:4]}/{tmp[4:6]}/{tmp[6:8]} {tmp[8:10]}:{tmp[10:12]}:{tmp[12:14]}.{tmp[14:]}'
    print(f'기준시각 : {date_in}이후')
    for i in res:
        print(f'센서:{i["_id"]}   최고:{i["max"]},  최저:{i["min"]},  평균:{i["avg"]}')
    

if(args.command == 'delete'):
    q = str(input('delete all? [Y/N]:'))
    if(q == 'Y' or q == 'y'):
        result = delete.delay()
        print('complete delete_all')
