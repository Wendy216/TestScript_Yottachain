#/use/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import datetime
import time
import commands
import logging
import threading

log_file = 'log.txt'

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3cmd_home = "/usr/bin/s3cmd"
bucket_list = []

def get_s3cmd_path():
    global s3cmd_home
    ret, output = commands.getstatusoutput("which s3cmd")
    if ret != 0:
        logger.error("fail to get s3cmd path.")
        sys.exit(1)
    s3cmd_home = output.strip("\n")
    return s3cmd_home


def check_upload(bucket_name):
    s3cmd_home = get_s3cmd_path()
    ret, output = commands.getstatusoutput("%s --no-ssl ls s3://%s" % (s3cmd_home, bucket_name))
    if ret != 0:
        logging.error("s3cmd ls s3://%s error." % bucket)
        sys.exit(1)


def download_file(bucket_name):
    s3cmd_home = get_s3cmd_path()
    check_upload(bucket_name)
    ret, output = commands.getstatusoutput("%s --no-ssl get s3://%s/* --force" % (s3cmd_home, bucket_name))
    if ret != 0:
        logger.error("Fail to download files in bucket %s" % bucket_name)
        sys.exit(2)
    logger.info("Download bucket %s successfully." % bucket_name)


def get_bucket_name():
    global bucket_list
    s3cmd_home = get_s3cmd_path()
    ret, output = commands.getstatusoutput("%s --no-ssl ls" % s3cmd_home)
    if ret != 0:
        logger.error("Fail to run s3cmd ls.")
        sys.exit(1)
    for name in output.split("\n"):
        bucket_name = name.split("//")[-1]
        bucket_list.append(bucket_name)
    print "bucket_list: ", bucket_list


def dateRange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    return dates


if __name__ == '__main__':

    logger.info("loading config...")

    config_dir_path = os.getcwd()
    print "config path: %s" % config_dir_path

    config_file_list = []
    config_file_num = 0
    list = os.listdir(config_dir_path)
    for file in list:
        #print file
        if file.startswith("config_download"):
            config_file_num += 1
            config_file_list.append(os.path.join(config_dir_path, file))
    print "config_file_list: %s" % config_file_list
    logger.info("config_file_list: %s" % config_file_list)

    if config_file_num == 0:
        logger.info("there is no config file, exit.")
        sys.exit(1)

    config_file_list_split = [i.split("/")[-1] for i in config_file_list]

    threads = []

    #循环读取配置文件，config1.txt, config2.txt.... 然后根据配置文件中file名称创建目录，在目录下生成对应数量的文件进行上传
    for config_file in config_file_list_split:
        # open config file to read bucket info, file name, file size and file number
        logger.info("open config file %s..." % config_file)
        with open(config_file, "r") as config:
            for line in config:
                content = line.strip("\n").split("=")
                if content[0] == "bucket_start":
                    bucket_start = content[1]
                elif content[0] == "bucket_end":
                    bucket_end = content[1]

        print "bucket_start: %s" % bucket_start
        print "bucket_end: %s" % bucket_end

        get_bucket_name()
        for date in dateRange(bucket_start, bucket_end):
            print date
            if date in bucket_list:
                t = threading.Thread(target=download_file, args=(date,))
                t.start()
