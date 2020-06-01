# -*- coding:utf-8 -*-
# @Time  : 2020/5/28  10:36
# @Author: rodgerlu


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import os
from pylab import *
import math
import xlrd
import csv
from xlutils.copy import copy  # 复制函数

mpl.rcParams['font.sans-serif'] = ['SimHei']


def count_data(path, record_time, mand_temp):
    record = {}
    log_files = os.listdir(path)
    for log_file in log_files:  # 循环读取路径下的文件并筛选输出
        if os.path.splitext(log_file)[1] == ".log":  # 筛选log文件
            if record_time in os.path.splitext(log_file)[0]:
                log_path = os.path.join(path, log_file)
                with open(log_path, 'r', encoding='utf-8') as log:
                    for line in log:
                        a = line.split('\t')
                        mand = a[5]
                        time = a[0]
                        if time not in record.keys():
                            record[time] = {mand_temp: 0, 'user': {}}
                        b = a[7].split('|')
                        user_date = json.loads(b[6])
                        if mand == mand_temp:
                            record[time][mand] += 1;
                            uid = user_date['uid']
                            user_record = record[time]['user']
                            if uid not in user_record.keys():
                                user_record[uid] = 1
                            else:
                                user_record[uid] += 1
    list = []
    for i in record:
        list.append(dict({'time': i}, **record[i]))
    return list


def show_record(record, mand):
    record = sorted(record, key=lambda kv: kv['time'])[:600]
    y_axis_data = []
    x_axis_data = []
    for i in record:
        x_axis_data.append(i['time'])
        y_axis_data.append(i[mand_one])
    plt.figure(figsize=(300, 10))  # 设置画布大小
    for a, b in zip(x_axis_data, y_axis_data):  # 给坐标点加标注
        plt.text(a, b, b, ha='center', va='bottom', fontsize=10)
    # fig, ax = plt.subplots(1, 1)
    # for label in ax.get_xticklabels():
    #     label.set_rotation(90)  # 旋转90度
    #     label.set_horizontalalignment('left')  # 向左旋转
    plt.plot(x_axis_data, y_axis_data, 'ro-', color='#4169E1', alpha=0.8, linewidth=1, label=mand)
    plt.legend(loc="upper right")
    plt.xlabel('time')
    plt.ylabel('QPS')
    plt.savefig(r'C:\Users\Administrator\Desktop\log\record_2020052301_02', bbox_inches='tight')


def show_proportion(path, mand_one):
    with open(path + mand_one + '.log', 'r+',
              encoding='UTF-8') as f:
        record = f.read()
        record = json.loads(record)
    proportion_dict = {}
    for i in record:
        if i[mand_one] not in proportion_dict.keys():
            proportion_dict[i[mand_one]] = 1
        else:
            proportion_dict[i[mand_one]] += 1
    proportion_list = sorted(proportion_dict.items(), key=lambda ky: ky[1])
    plt.figure(figsize=(9, 9))  # 将画布设定为正方形，则绘制的饼图是正圆
    label = []
    values = []
    for i in proportion_list:
        label.append(i[0])  # 定义饼图的标签，标签是列表
        values.append(i[1])
    explode = [0.1] * len(label)
    plt.pie(values, explode=explode, labels=label, autopct='%1.3f%%')  # 绘制饼图
    plt.title('QPS分布图')  # 绘制标题
    plt.savefig(path + mand_one, bbox_inches='tight')  # 保存图片
    plt.show()


def show_uid_proportion(path, mand, times):
    with open(path + mand + '.log', 'r', encoding='UTF-8') as f:
        record = f.read()
        record = json.loads(record)
        for i in times:
            for re in record:
                if i == re['time']:
                    b = i.replace('[', '').replace(']', '').replace(':', '')
                    user = re['user']
                    excelDir = path + mand + b + '.csv'
                    excelDir = excelDir.replace(' ', '')
                    with open(excelDir, 'w', encoding='utf-8', newline='') as cf:
                        csv_wr = csv.writer(cf)
                        csv_wr.writerow(['uid', 'num'])
                        user = sorted(user.items(), key=lambda ky: ky[1], reverse=True)
                        label = []
                        values = []
                        for u in user:
                            label.append(u[0])
                            values.append(u[1])
                            csv_wr.writerow(u)

                    plt.figure(figsize=(9, 9))  # 将画布设定为正方形，则绘制的饼图是正圆
                    explode = [0.1] * len(label)
                    plt.pie(values, explode=explode, labels=label, autopct='%1.3f%%')  # 绘制饼图
                    plt.title(i + " " + mand)  # 绘制标题
                    plt.savefig(path + mand + b, bbox_inches='tight')  # 保存图片


if __name__ == "__main__":
    # logfile = r'C:\Users\Administrator\Downloads'
    mand_one = 'get_rank'
    mand_two = 'get_alliance_war_al_score'

    # 饼状图保存
    path = r'C:\Users\Administrator\Desktop\log1\record_2020052312_13_'
    # show_proportion(path,mand_two)
    times = ['[2020-05-23 02:30:00]', '[2020-05-23 01:00:01]', '[2020-05-23 02:29:59]', '[2020-05-23 02:30:01]',
             '[2020-05-23 02:30:02]', '[2020-05-23 01:00:00]']
    times1 = ['[2020-05-23 13:30:00]', '[2020-05-23 13:30:01]', '[2020-05-23 12:00:01]', '[2020-05-23 13:29:59]',
              '[2020-05-23 12:00:02]', '[2020-05-23 13:30:02]']
    times2 = ['[2020-05-23 22:30:00]', '[2020-05-23 22:30:01]', '[2020-05-23 21:00:01]', '[2020-05-23 22:29:59]',
              '[2020-05-23 22:30:02]', '[2020-05-23 21:00:02]', "[2020-05-23 22:30:03]", "[2020-05-23 21:00:00]"
              ]
    times3 = ['[2020-05-23 02:30:00]', '[2020-05-23 01:00:01]', '[2020-05-23 02:29:59]', '[2020-05-23 02:30:01]',
              '[2020-05-23 02:30:02]', '[2020-05-23 01:00:00]']
    times4 = ['[2020-05-23 13:30:00]', '[2020-05-23 12:00:01]', '[2020-05-23 13:29:59]', '[2020-05-23 13:30:01]',
              '[2020-05-23 12:00:00]', '[2020-05-23 12:03:00]', '[2020-05-23 13:30:02]']
    times5 = ['[2020-05-23 22:30:00]', '[2020-05-23 21:00:01]', '[2020-05-23 22:29:59]', '[2020-05-23 22:30:01]',
              '[2020-05-23 22:30:02]', '[2020-05-23 21:00:00]', '[2020-05-23 21:03:00]']
    show_uid_proportion(path, mand_two, times4)

    # # 1、读取excel
    # path = r'C:\Users\Administrator\Desktop\log1\record_2020052321_22_' + mand_one  # excel文件路径
    # excelDir = path + '.xls'
    # # 打开excel
    # workbook = xlrd.open_workbook(excelDir)
    # workSheet = workbook.sheet_by_name('Sheet1')
    # # 2、写入excel
    # from xlutils.copy import copy  # 复制函数
    #
    # workbookWr = copy(workbook)  # 拷贝一个副本
    # wrSheet = workbookWr.get_sheet(0)  # 获取第一个sheet
    #
    # wrSheet.write(0, 0, '时间')  # 写入单元格
    # wrSheet.write(0, 1, 'QPS')
    # with open(path + '.log', 'r',
    #           encoding='UTF-8') as f:
    #     record = f.read()
    #     record = json.loads(record)
    #     k = 1
    #     for i in record:
    #         wrSheet.write(k, 0, i['time'])
    #         wrSheet.write(k, 1, i[mand_one])
    #         k += 1
    # # 保存数据，将文件后缀名保存为.xls格式后，文件才能打开
    # workbookWr.save(path + '.xls')

    # for i in record:
    #     i[mand_two] = math.ceil(float(i[mand_two]) / 10) * 10  # 向上取整
    # file = open(r'C:\Users\Administrator\Desktop\log1\record_2020052301_02_' + mand_two + '.log', 'w', encoding='UTF-8')
    # record = json.dumps(record)
    # file.write(record)
    # file.close()

    # record_2020052301_02 = count_data(logfile, '2020052301', mand_one) + count_data(logfile, '2020052302', mand_one)
    # record_2020052312_13 = count_data(logfile, '2020052312', mand_one) + count_data(logfile, '2020052313', mand_one)
    # record_2020052321_22 = count_data(logfile, '2020052321', mand_one) + count_data(logfile, '2020052322', mand_one)
    # record_2020052301_02 = json.dumps(sorted(record_2020052301_02, key=lambda kv: kv['time']))
    # record_2020052312_13 = json.dumps(sorted(record_2020052312_13, key=lambda kv: kv['time']))
    # record_2020052321_22 = json.dumps(sorted(record_2020052321_22, key=lambda kv: kv['time']))
    # file = open(r'C:\Users\Administrator\Desktop\log\record_2020052301_02_' + mand_one + '.log', 'w', encoding='UTF-8')
    # file.write(record_2020052301_02)
    # file = open(r'C:\Users\Administrator\Desktop\log\record_2020052312_13_' + mand_one + '.log', 'w', encoding='UTF-8')
    # file.write(record_2020052312_13)
    # file = open(r'C:\Users\Administrator\Desktop\log\record_2020052321_22_' + mand_one + '.log', 'w', encoding='UTF-8')
    # file.write(record_2020052321_22)
    # file.close()

    # record_2020052301_02 = count_data(logfile, '2020052301', mand_two) + count_data(logfile, '2020052302', mand_two)
    # record_2020052312_13 = count_data(logfile, '2020052312', mand_two) + count_data(logfile, '2020052313', mand_two)
    # record_2020052321_22 = count_data(logfile, '2020052321', mand_two) + count_data(logfile, '2020052322', mand_two)
    # record_2020052301_02 = json.dumps(sorted(record_2020052301_02, key=lambda kv: kv['time']))
    # record_2020052312_13 = json.dumps(sorted(record_2020052312_13, key=lambda kv: kv['time']))
    # record_2020052321_22 = json.dumps(sorted(record_2020052321_22, key=lambda kv: kv['time']))
    # file = open(r'C:\Users\Administrator\Desktop\log\record_2020052301_02_' + mand_two + '.log', 'w', encoding='UTF-8')
    # file.write(record_2020052301_02)
    # file = open(r'C:\Users\Administrator\Desktop\log\record_2020052312_13_' + mand_two + '.log', 'w', encoding='UTF-8')
    # file.write(record_2020052312_13)
    # file = open(r'C:\Users\Administrator\Desktop\log\record_2020052321_22_' + mand_two + '.log', 'w', encoding='UTF-8')
    # file.write(record_2020052321_22)
    # file.close()
