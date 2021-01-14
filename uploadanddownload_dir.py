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
version_file="version.conf"
l_c = Lock()
datanode_number = 0

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
    fp.append_version(report_file, version_file)

    list_config = dp.get_config()
    for config in list_config:
        if "config_dir" in config["configfilename"]:
            bucket_name = config["bucket_name"]
            file_size = config["file_size"]
            file_number = config["file_number"]
            print "bucket_name: %s" % bucket_name
            print "file_size: %s" % file_size
            print "file_number: %s" % file_number

            uuid_str = uuid.uuid4().hex[0:6]
            tmp_file_name = 'tmpfile_%s_%s' % (bucket_name, uuid_str)
            dir_name = tmp_file_name
            print "dir_name: %s" % dir_name

            logger.info("Begin to run test dir %s upload and download." % dir_name)
            fp.append_report(report_file, "\n\n[%s] - Begin to run test dir %s upload and download.\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name))

            file_name = []
            for i in range(int(file_number)):
                file_name.append(dir_name + ".txt%s" % i)
            print "file_name: %s" % file_name

            dp.p_u_d(dir_name, int(file_size), bucket_name, int(file_number), file_name, datanode_number, False, False)
