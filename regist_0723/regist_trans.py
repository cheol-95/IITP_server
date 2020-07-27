def read_file(file):
    try:
        frd = open(file, mode='rt', encoding='utf-8') #newline=None
        print(f'{file} read succese!')
    except OSError as err:
        print(f"OS error: {err}")
    tmp = frd.read(4123)
    frd.close()
    return tmp

file = 'tasks'
test = read_file(file + '.py').\
    replace('regist', 'set_sensor').\
    replace('get_sensorInfo', 'get_sensor').\
    replace('insert_key', 'set_key').\
    replace('get_keyList', 'get_key').\
    replace('update', 'update_sensor').\
    replace('delete', 'delete_sensor')

f = open(file + '_trans.py', mode='wt', encoding='utf-8')
f.write(test)
f.close()