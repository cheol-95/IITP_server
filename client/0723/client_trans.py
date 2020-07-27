def read_file(file):
    try:
        frd = open(file, mode='rt', encoding='utf-8') #newline=None
        print(f'{file} read succese!')
    except OSError as err:
        print(f"OS error: {err}")
    tmp = frd.read(10000)
    frd.close()
    return tmp

file = 'tasks'
test = read_file(file + '.py').\
    replace('insert', 'set_data').\
    replace('maxmin', 'get_redis_data').\
    replace('getSensorInfo_Mongo', 'get_sensor_mongo').\
    replace('getSensorInfo_Redis', 'get_sensor_redis').\
    replace('find', 'get_data').\
    replace('summary', 'get_summary').\
    replace('analy', 'get_analy').\
    replace('analy_d', 'get_date_analy').\
    replace('delete', 'delete_data')

f = open(file + '_trans.py', mode='wt', encoding='utf-8')
f.write(test)
f.close()