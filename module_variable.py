#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import commands
import logging

log_file = 'log.txt'
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3cmd_home = "/usr/bin/s3cmd"
def get_s3cmd_path():
    global s3cmd_home
    ret, output = commands.getstatusoutput("which s3cmd")
    if ret != 0:
        logger.error("fail to get s3cmd path.")
        sys.exit(1)
    s3cmd_home = output.strip("\n")
    return s3cmd_home