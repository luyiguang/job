# -*- coding:utf-8 -*-
# @Time  : 2020/5/29  11:14
# @Author: rodgerlu
import os
import time
import datetime
import configparser
import pymysql
import json

event_type_dic = {'6': 'first_rech', '7': 'beginner_accu', '8': 'beginner_date', '9': 'date_rech',
                  '10': 'accumlate_rech', '11': 'user_accu', '12': 'dragon_trial', '13': 'battle_pass',
                  '14': 'battle_of', '15': 'alliance_war', '16': 'joy_event'}


def get_record(file, mand):
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.replace('\n', '')
            a = line.split('\t')
            data_time = a[0].replace('[', '').replace(']', '')
            b = a[5].split('|')
            if mand == b[0]:
                c = b[2:]
                for i in c:
                    if ",0" in i:
                        if mand == 'add_score':
                            user = b[1]
                            acts = b[2:]
                            act = []
                            for j in acts:
                                if ',0' in j:
                                    act.append(j)
                            yield data_time, user, act
                        if mand == 'batch_add_score':
                            b[1] = json.loads(b[1])
                            user = b[1][int(b[2])]
                            acts = b[3:]
                            act = []
                            for j in acts:
                                if ',0' in j:
                                    act.append(j)
                            yield data_time, user, act
                        break


def get_files_name(dir):
    files =''
    for root, dirs, files in os.walk(dir):  # 获取目录下所有文件信息
        files = files
    for file in files:
        file_name = os.path.splitext(file)[0]
        file_extend = os.path.splitext(file)[1]
        if file_extend == '.ok':
            if 'event_center_new' in file_name:
                yield file_name


def init_table(today):
    with pymysql.connect(**config) as cur:
        for event in event_type_dic.values():
            table_name = event + '_' + today.replace('-', '_')
            sql = '''create table if not exists {0} (
                id int primary key AUTO_INCREMENT ,
                mand varchar(20),
                aid varchar(20),
                detail varchar (20),
                score varchar (100),
                type varchar (20),
                sid varchar (20),
                uid varchar (20) not null ,
                time timestamp not null,
                event varchar(50) 
                )'''.format(table_name)
            cur.execute(sql)


def data_mining(dir, mand, today: str, update_time: str):
    insert_data = {}
    for type in event_type_dic.values():
        insert_data[type] = []
    for i in get_files_name(dir):
        record = get_record(os.path.join(dir, i).replace('\\','/'), mand)
        for data_time, user, event in record:
            if data_time.split(' ')[0] == today:
                if not isinstance(user, dict):
                    user = json.loads(user)
                event_type = event[0].split(',')[0]
                event_name = event_type_dic[event_type]
                insert_data[event_name].append((mand, user['aid'],
                                                json.dumps(user['detail']).replace(' ', '').replace('[', '').replace(
                                                    ']',
                                                    ''),
                                                json.dumps(user['score']).replace(' ', '').replace('[', '').replace(']',
                                                                                                                    ''),
                                                user['type'], user['sid'], user['uid'], data_time, event[0]))
    for type in event_type_dic.values():
        table_name = type + '_' + today.replace('-', '_')
        insert_temp = '''
            insert into {0} (mand,aid,detail,score,type,sid,uid,time,event)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''.format(table_name)
        with pymysql.connect(**config) as cur:
            update_one_hour = datetime.datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)
            update_one_hour = update_one_hour.strftime('%Y-%m-%d %H:%M:%S')
            update_sql = "delete from {0} where time between '{1}' and '{2}' and mand ='{3}'".format(table_name,
                                                                                                     update_time,
                                                                                                     update_one_hour,
                                                                                                     mand)
            cur.execute(update_sql)
            cur.executemany(insert_temp, insert_data[type])


def db_config_generate():
    config = configparser.ConfigParser()
    config.add_section("DB")
    config.set("DB", "Host", "localhost")
    config.set("DB", "Port", "3306")
    config.set("DB", "Database", "rodger")
    config.set("DB", "User", "root")
    config.set("DB", "Password", "root")
    config.add_section("TIME")
    config.set("TIME", "Year", "2020")
    config.set("TIME", "Month", "05")
    config.set("TIME", "Day", "28")
    config.set("TIME", "Hour", "00")
    config.set("TIME", "Minue", "00")
    config.set("TIME", "Second", "00")
    config.add_section("FILE")
    config.set("FILE", "Dir", r"E:\sCrpDownload")
    config.set("FILE", "Mand", "add_score")
    with open("database.conf", "w+", encoding="utf-8") as f:
        config.write(f)


def db_config_read(config_dic):
    config = configparser.ConfigParser()
    if not os.path.exists("database.conf"):
        db_config_generate()
    config.read('database.conf', encoding="utf-8")
    config_dic['db_host'] = config.get('DB', 'Host')
    config_dic['db_user'] = config.get('DB', 'User')
    config_dic['db_password'] = config.get('DB', 'Password')
    config_dic['db_port'] = config.getint('DB', 'Port')
    config_dic['db_name'] = config.get('DB', 'Database')
    config_dic['year'] = config.get('TIME','Year')
    config_dic['month'] = config.get('TIME', 'Month')
    config_dic['day'] = config.get('TIME', 'Day')
    config_dic['hour'] = config.get('TIME', 'Hour')
    config_dic['minue'] = config.get('TIME', 'Minue')
    config_dic['second'] = config.get('TIME', 'Second')
    config_dic['dir'] = config.get("FILE", 'Dir')
    config_dic['mand'] = config.get("FILE", "Mand")


if __name__ == "__main__":
    # DaysAgo = (datetime.datetime.now() - datetime.timedelta(days=5))
    # # 转换为其他字符串格式
    # today = DaysAgo.strftime("%Y-%m-%d")
    #
    # today = '2020-05-28'
    # update_time = "2020-05-28 00:00:00"
    config_dic = {}
    db_config_read(config_dic)
    today = config_dic['year']+'-'+config_dic['month']+'-'+config_dic['day']
    config = {'host': config_dic['db_host'], 'port': config_dic['db_port'], 'password': config_dic['db_password'],
              'db': config_dic['db_name'], "user": config_dic['db_user'], 'charset': 'utf8'}
    init_table(today)
    mand = 'add_score'
    mand1 = 'batch_add_score'
    for h in range(24):
        hour = str(h).zfill(2)
        dir_name = today.replace('-','')+hour
        dir = os.path.join(config_dic['dir'],dir_name).replace('\\','/')
        update_time = today+' '+hour+":"+"00:00"
        data_mining(dir, mand, today, update_time)
        data_mining(dir, mand1, today, update_time)
