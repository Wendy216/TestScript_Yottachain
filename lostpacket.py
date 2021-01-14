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
import datetime
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

def usage():
    print "usage: python lostpacket.py [report_file] eth0/docker0 loss_ration"
    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) == 4:
        report_file = sys.argv[1]
        fp.lost_packet(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 3:
        fp.lost_packet(sys.argv[1], sys.argv[2])
    else:
        usage()
    logger.info("loading config...")

    list_config = dp.get_config()
    for config in list_config:
        if "config_dir" in config["configfilename"]:
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

            logger.info("\nBegin to run lost packet.")
            fp.append_report(report_file, "\n\n[%s] - begin to run test lost packet.\n"
                          % (time.strftime("%Y-%m-%d %H:%M:%S")))

            for i in range(int(file_number)):
                t = threading.Thread(target=fp.c_u_d,
                                     args=(file_name + ".txt%s" % i, int(file_size), bucket_name, transfer_way,
                                           access_key, secret_key))
                t.start()
                threads.append(t)

            for th in threads:
                th.join()

            fp.append_report(report_file, "[%s] - Finish to run test file number=%d file size=%d upload and download.\n\n"
                      % (time.strftime("%Y-%m-%d %H:%M:%S"), int(file_number), int(file_size)))