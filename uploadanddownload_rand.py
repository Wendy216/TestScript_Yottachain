#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import os
import time
import shutil
import logging
import commands
import threading
import uuid
import random
from threading import Lock
import fileoperation as fp
import diroperation as dp

file_number = 10
file_size = 10
log_file = 'log.txt'
#s3cmd_home = "/usr/bin/s3cmd"
report_file = "report.txt"
version_file = "version.txt"
l_c = Lock()
key_file = "key.txt"

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

if __name__ == '__main__':

    logger.info("loading config...")
    if len(sys.argv) == 2:
        report_file = sys.argv[1]
    print "report_file: %s" % report_file

    fp.append_version(report_file, version_file)

    config_dir_path = os.getcwd()
    print "config path: %s" % config_dir_path

    config_file_list = []
    config_file_num = 0
    list = os.listdir(config_dir_path)
    for file in list:
        #print file
        if "config_rand" in file:
            config_file_num += 1
            config_file_list.append(os.path.join(config_dir_path, file))
    print "config_file_list: %s" % config_file_list
    logger.info("config_file_list: %s" % config_file_list)

    if config_file_num == 0:
        logger.info("there is no config file, exit.")
        sys.exit(1)

    # transfer_way, access_key, secret_key in another file
    with open(key_file, "r") as key:
        for line in key:
            content = line.strip("\n").split("=")
            if content[0] == "transfer_way":
                transfer_way = content[1]
            elif content[0] == "access_key":
                access_key = content[1]
            elif content[0] == "secret_key":
                secret_key = content[1]

    config_file_list_split = [i.split("/")[-1] for i in config_file_list]

    # 循环读取配置文件，config1.txt, config2.txt.... 然后根据配置文件中file名称创建目录，在目录下生成对应数量的文件进行上传
    for config_file in config_file_list_split:
        # open config file to read bucket info, file name, file size and file number
        logger.info("open config file %s..." % config_file)
        with open(config_file, "r") as config:
            for line in config:
                content = line.strip("\n").split("=")
                if content[0] == "bucket":
                    bucket_name = content[1]
                elif content[0] == "number":
                    file_number = content[1]
                elif content[0] == "l_size":
                    l_size = content[1]
                elif content[0] == "h_size":
                    h_size = content[1]

        print "bucket_name: %s" % bucket_name
        print "file_number: %s" % file_number
        print "transfer_way: %s" % transfer_way

        uuid_str = uuid.uuid4().hex[0:6]
        tmp_file_name = 'tmpfile_%s_%s' % (bucket_name, uuid_str)
        file_name = tmp_file_name

        threads = []

        logger.info("Begin to run test file number=%d upload and download." % (int(file_number)))
        fp.append_report(report_file, "\n\n[%s] - Begin to run test file number=%d upload and download.\n"
                      % (time.strftime("%Y-%m-%d %H:%M:%S"), int(file_number)))

        for i in range(int(file_number)):
            file_size = random.randint(int(l_size), int(h_size))
            logger.info("\nBegin to run test file size=%d upload and download." % int(file_size))
            fp.append_report(report_file, "[%s] - Begin to run test file size=%d upload and download.\n"
                          % (time.strftime("%Y-%m-%d %H:%M:%S"), int(file_size)))
            t = threading.Thread(target=fp.c_u_d, args=(file_name + ".txt%s" % i, int(file_size), bucket_name,
                                                        transfer_way, access_key, secret_key))
            t.start()
            threads.append(t)

        for th in threads:
            th.join()

        fp.append_report(report_file, "[%s] - Finish to run test file number=%d upload and download.\n\n"
                      % (time.strftime("%Y-%m-%d %H:%M:%S"), int(file_number)))