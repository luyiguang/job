# -*- coding:utf-8 -*-
# @Time  : 2020/5/29  11:14
# @Author: rodgerlu
import os
import time
import pymysql
import json


def get_record(file, mand):
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.replace('\n', '')
            a = line.split('\t')
            time = a[0].replace('[', '').replace(']', '')
            b = a[5].split('|')
            if mand == b[0]:
                c = b[2:]
                for i in c:
                    if ",0" in i:
                        if mand == 'add_score':
                            user = b[1]
                            act = b[2:]
                            yield time, user, act
                        if mand == 'batch_add_score':
                            b[1] = json.loads(b[1])
                            user = b[1][int(b[2])]
                            act = b[3:]
                            yield time, user, act
                        break


def get_files_name(dir):
    for root, dirs, files in os.walk(dir):  # 获取目录下所有文件信息
        files = files
    for file in files:
        file_name = os.path.splitext(file)[0]
        file_extend = os.path.splitext(file)[1]
        if file_extend == '.ok':
            if 'event_center_new' in file_name:
                yield file_name


if __name__ == "__main__":
    # Read fime name
    file_name = r'E:\sCrpDownload\event_center_new_2020052812.log_172.31.4.144.log'
    mand_one = 'add_score'
    mand_two = 'batch_add_score'
    dir = r"E:\sCrpDownload"
    for i in get_files_name(dir):
        record = get_record(os.path.join(dir, i), mand_two)
        for time, user, act in record:
            print(time, user)
            acts =[]
            for j in act:
                if ',0' in j:
                  acts.append(j)
            print(act)
            print(acts)
    # config = {'host':"localhost",'port':3306,'password':'root','db':'rodger',"user":'root','charset':'utf8'}
    # with pymysql.connect(**config) as conn:

    # # print file creation time
    # print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.stat(FileName).st_atime)))
    #
    # # print file modified time
    # print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.stat(FileName).st_mtime)))
    # if int(os.stat(FileName).st_atime) == int(os.stat(FileName).st_mtime):
    #     print('File has not been modified.')
    # print(os.path.getmtime(FileName))
