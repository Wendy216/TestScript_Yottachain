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
from threading import Lock
import fileoperation as fp
import diroperation as dp

file_number = 10
file_size = 10
log_file = 'log.txt'
#s3cmd_home = "/usr/bin/s3cmd"
report_file = "report.txt"
version_file="version.txt"
l_c = Lock()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

if __name__ == "__main__":
    logger.info("loading config...")
    if len(sys.argv) == 2:
        report_file = sys.argv[1]
    print "report_file: %s" % report_file

    fp.append_version(report_file, version_file)
    list_config = dp.get_config()
    for config in list_config:
        if "config_upload" in config["configfilename"]:
            bucket_name = config["bucket_name"]
            file_size = config["file_size"]
            file_number = config["file_number"]
            transfer_way = config["transfer_way"]
            access_key = config["access_key"]
            secret_key = config["secret_key"]

            print "bucket_name: %s" % bucket_name
            print "file_size: %s" % file_size
            print "file_number: %s" % file_number
            print "transfer_way: %s" % transfer_way

            uuid_str = uuid.uuid4().hex[0:6]
            tmp_file_name = 'tmpfile_%s_%s' % (bucket_name, uuid_str)
            file_name = tmp_file_name

            threads = []

            logger.info(
                "Begin to run test file number=%d size=%d upload." % (int(file_number), int(file_size)))
            fp.append_report(report_file, "\n\n[%s] - begin to run test file number=%d file size=%d upload.\n"
                          % (time.strftime("%Y-%m-%d %H:%M:%S"), int(file_number), int(file_size)))

            for i in range(int(file_number)):
                t = threading.Thread(target=fp.c_u,
                                     args=(file_name + ".txt%s" % i,
                                           int(file_size), bucket_name, transfer_way, access_key, secret_key))
                t.start()
                threads.append(t)

            for th in threads:
                th.join()

            fp.append_report(report_file, "[%s] - Finish to run test file number=%d file size=%d upload.\n"
                             % (time.strftime("%Y-%m-%d %H:%M:%S"), int(file_number), int(file_size)))

