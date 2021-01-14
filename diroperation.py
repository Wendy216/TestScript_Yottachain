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
import module_variable

file_number = 10
file_size = 10
log_file = 'log.txt'
#s3cmd_home = "/usr/bin/s3cmd"
report_file = "report.txt"
version_file ="version.txt"
key_file = "key.txt"
l_c = Lock()
datanode_number = 0
max_times = 0

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3cmd_home = module_variable.get_s3cmd_path()

# before download check s3cmd ls s3://bucket_name/file_name whether the file has been uploaded
def check_dir_file_upload(bucket, dir_name, file_name):
    find_upload = False

    ret, output = commands.getstatusoutput("%s --no-ssl ls s3://%s/%s/%s" % (s3cmd_home, bucket, dir_name, file_name))
    if ret != 0:
        logging.error("s3cmd ls s3://%s error." % bucket)

    if output == "":
        find_upload = False
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            result = line.split("/")[-1]
            if file_name == result:
                find_upload = True
                logger.info("upload %s successfully." % file_name)
                break
    return find_upload


def check_dir_upload(bucket, dir_name, file_number):
    dir_upload = False
    ret, output = commands.getstatusoutput("%s --no-ssl ls s3://%s/%s/ | wc -l" % (s3cmd_home, bucket, dir_name))
    if ret != 0:
        logging.error("s3cmd ls s3://%s/%s error." % (bucket, dir_name))
        return
    if int(output) == 0:
        dir_upload = False
    else:
        file_number_in_bucket = int(output)
        if file_number_in_bucket == file_number:
            dir_upload = True
            logger.info("dir %s upload successfully." % dir_name)
        else:
            dir_upload = False
    return dir_upload


# upload file to the bucket specified in config file
def upload_dir_report(report_file, bucket, dir_name, file_number, datanode_number, restartyts3, restartytsn):
    check = False
    if fp.check_bucket(bucket):
        logger.info("call s3cmd to upload dir %s to bucket %s." % (dir_name, bucket))
        time_start = time.time()
        ret, output = commands.getstatusoutput("%s --no-ssl put --recursive %s s3://%s" % (s3cmd_home, dir_name, bucket))
        if ret != 0:
            logger.error("upload dir %s error!", dir_name)
            return
        time_end = time.time()
        total_time = time_end - time_start
        logger.info("upload dir %s totally cost %f" % (dir_name, total_time))
    time.sleep(2)
    if int(datanode_number) > 0:
        fp.close_partial_datanode(datanode_number)
    if restartyts3 == True:
        print "restart yts3"
        fp.restart_yts3()
    if restartytsn == True:
        print "restart ytsn"
        fp.restart_ytsn()

    if file_number <= 10:
        wait_time = 600
    elif file_number > 10:
        wait_time = 1200
    time_start = time.time()
    check = check_dir_upload(bucket, dir_name, file_number)
    while not check:
        time_end = time.time()
        if time_end - time_start > wait_time:
            break
        check = check_dir_upload(bucket, dir_name, file_number)
    if check == True:
        fp.append_report(report_file,
                      "[%s] - upload_dir_%s_file_number=%d_Pass\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))
    else:
        fp.append_report(report_file,
                      "[%s] - upload_dir_%s_file_number=%d_Fail\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))
# upload file to the bucket specified in config file
def upload_dir(bucket, dir_name, file_number, datanode_number, restartyts3, restartytsn):
    check = False
    if fp.check_bucket(bucket):
        logger.info("call s3cmd to upload dir %s to bucket %s." % (dir_name, bucket))
        time_start = time.time()
        ret, output = commands.getstatusoutput("%s --no-ssl put --recursive %s s3://%s" % (s3cmd_home, dir_name, bucket))
        if ret != 0:
            logger.error("upload dir %s error!", dir_name)
            return
        time_end = time.time()
        total_time = time_end - time_start
        logger.info("upload dir %s totally cost %f" % (dir_name, total_time))
    time.sleep(2)
    if int(datanode_number) > 0:
        fp.close_partial_datanode(datanode_number)
        time.sleep(10)
    if restartyts3 == True:
        print "restart yts3"
        fp.restart_yts3()
        time.sleep(10)
    if restartytsn == True:
        print "restart ytsn"
        fp.restart_ytsn()
        time.sleep(10)
    check = check_dir_upload(bucket, dir_name, file_number)
    while not check:
        check = check_dir_upload(bucket, dir_name, file_number)
    if check == True:
        fp.append_report(report_file,
                      "[%s] - upload_dir_%s_file_number=%d_Pass\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))
    else:
        fp.append_report(report_file,
                      "[%s] - upload_dir_%s_file_number=%d_Fail\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))


# upload file to the bucket specified in config file
def upload_dir_docker(bucket, dir_name, file_number, datanode_number, restartyts3, restartytsn):
    check = False
    if fp.check_bucket(bucket):
        logger.info("call s3cmd to upload dir %s to bucket %s." % (dir_name, bucket))
        time_start = time.time()
        ret, output = commands.getstatusoutput("%s --no-ssl put --recursive %s s3://%s" % (s3cmd_home, dir_name, bucket))
        if ret != 0:
            logger.error("upload dir %s error!", dir_name)
            return
        time_end = time.time()
        total_time = time_end - time_start
        logger.info("upload dir %s totally cost %f" % (dir_name, total_time))
    time.sleep(2)
    if int(datanode_number) > 0:
        fp.close_partial_datanode(datanode_number)
        time.sleep(10)
    if restartyts3 == True:
        print "restart yts3"
        fp.restart_yts3_docker()
        time.sleep(10)
    if restartytsn == True:
        print "restart ytsn"
        fp.restart_ytsn()
        time.sleep(10)
    check = check_dir_upload(bucket, dir_name, file_number)
    while not check:
        check = check_dir_upload(bucket, dir_name, file_number)
    if check == True:
        fp.append_report(report_file,
                      "[%s] - upload_dir_%s_file_number=%d_Pass\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))
    else:
        fp.append_report(report_file,
                      "[%s] - upload_dir_%s_file_number=%d_Fail\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))


def upload_dir_container(bucket, dir_name, file_number, datanode_number, restartyts3, restartytsn):
    check = False
    if fp.check_bucket(bucket):
        logger.info("call s3cmd to upload dir %s to bucket %s." % (dir_name, bucket))
        time_start = time.time()
        ret, output = commands.getstatusoutput(
            "%s --no-ssl put --recursive %s s3://%s" % (s3cmd_home, dir_name, bucket))
        if ret != 0:
            logger.error("upload dir %s error!", dir_name)
            return
        time_end = time.time()
        total_time = time_end - time_start
        logger.info("upload dir %s totally cost %f" % (dir_name, total_time))
    time.sleep(2)
    if int(datanode_number) > 0:
        fp.close_partial_datanode(datanode_number)
        time.sleep(10)
    if restartyts3 == True:
        print "restart yts3"
        fp.restart_yts3_container()
        time.sleep(10)
    if restartytsn == True:
        print "restart ytsn"
        fp.restart_ytsn()
        time.sleep(10)
    check = check_dir_upload(bucket, dir_name, file_number)
    while not check:
        check = check_dir_upload(bucket, dir_name, file_number)
    if check == True:
        fp.append_report(report_file,
                         "[%s] - upload_dir_%s_file_number=%d_Pass\n" % (
                         time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))
    else:
        fp.append_report(report_file,
                         "[%s] - upload_dir_%s_file_number=%d_Fail\n" % (
                         time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_number))



#  download file from bucket and check whether the md5sum is same
def download_dir_file(bucket, dir_name, file_names, size):
    for file_name in file_names:
        upload_file_size = os.path.getsize(dir_name + "/" + file_name)
        download_file_name = file_name + ".down"
        find = check_dir_file_upload(bucket, dir_name, file_name)
        if not find:
            logger.error("the file %s is not exist in bucket %s" % (file_name, bucket))
            fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_Fail.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name))
            return
        else:
            ret, output = commands.getstatusoutput(
                "%s --no-ssl get s3://%s/%s/%s %s" % (s3cmd_home, bucket, dir_name, file_name, download_file_name))
            if ret != 0:
                fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_Fail.\n" % (
                              time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name))
                logger.error("download_dir_%s_file_%s_fail.\n" % (dir_name, file_name))
                return

        if not os.path.exists(download_file_name):
            logger.info("the download file %s is not exist." % file_name)
            fp.append_report(report_file, "[%s] - download_file_%s_size=%d_is not exist.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
            return
        download_file_size = os.path.getsize(download_file_name)
        if upload_file_size == download_file_size:
            fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_size=%d_Pass.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size))
            logger.info("download dir %s file %s successfully." % (dir_name, file_name))
        else:
            fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_size=%d_Fail.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size))
            return

        ret, output = commands.getstatusoutput("diff %s/%s %s" % (dir_name, file_name, download_file_name))
        if ret != 0:
            logger.error("files %s and %s are not same." % (file_name, download_file_name))
            fp.append_report(report_file, "[%s] - compare download_dir_%s_file_%s_size=%d and %s_Fail.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size, download_file_name))
            return
        else:
            logger.info("files %s and %s.down are same." % (file_name, file_name))
            fp.append_report(report_file, "[%s] - compare download_dir_%s_file_%s_size=%d and %s_Pass.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size, download_file_name))
        ret, output = commands.getstatusoutput("rm -f %s" % (download_file_name))
        if ret == 0:
            logger.info("remove file %s successfully." % (download_file_name))

    ret, output = commands.getstatusoutput("rm -rf %s" % (dir_name))
    if ret == 0:
        logger.info("remove dir %s successfully." % (dir_name))


def download_dir_file_report(report_file, bucket, dir_name, file_names, size):
    for file_name in file_names:
        upload_file_size = os.path.getsize(dir_name + "/" + file_name)
        download_file_name = file_name + ".down"
        find = check_dir_file_upload(bucket, dir_name, file_name)
        if not find:
            logger.error("the file %s is not exist in bucket %s" % (file_name, bucket))
            fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_Fail.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name))
            return
        else:
            ret, output = commands.getstatusoutput(
                "%s --no-ssl get s3://%s/%s/%s %s" % (s3cmd_home, bucket, dir_name, file_name, download_file_name))
            if ret != 0:
                fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_Fail.\n" % (
                              time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name))
                logger.error("download_dir_%s_file_%s_fail.\n" % (dir_name, file_name))
                return

        if not os.path.exists(download_file_name):
            logger.info("the download file %s is not exist.")
            append_report(report_file, "[%s] - download_file_%s_size=%d_is not exist.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
            return
        download_file_size = os.path.getsize(download_file_name)
        if upload_file_size == download_file_size:
            fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_size=%d_Pass.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size))
            logger.info("download dir %s file %s successfully." % (dir_name, file_name))
        else:
            fp.append_report(report_file, "[%s] - download_dir_%s_file_%s_size=%d_Fail.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size))
            return

        ret, output = commands.getstatusoutput("diff %s/%s %s" % (dir_name, file_name, download_file_name))
        if ret != 0:
            logger.error("files %s and %s are not same." % (file_name, download_file_name))
            fp.append_report(report_file, "[%s] - compare download_dir_%s_file_%s_size=%d and %s_Fail.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size, download_file_name))
            return
        else:
            logger.info("files %s and %s.down are same." % (file_name, file_name))
            fp.append_report(report_file, "[%s] - compare download_dir_%s_file_%s_size=%d and %s_Pass.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), dir_name, file_name, size, download_file_name))
        ret, output = commands.getstatusoutput("rm -f %s" % (download_file_name))
        if ret == 0:
            logger.info("remove file %s successfully." % (download_file_name))

    ret, output = commands.getstatusoutput("rm -rf %s" % (dir_name))
    if ret == 0:
        logger.info("remove dir %s successfully." % (dir_name))


def prepare_dir(dir_name, file_size, file_name):
    if os.path.exists(dir_name):
        ret, output = commands.getstatusoutput("rm -rfv %s" % dir_name)
        if ret != 0:
            logger.error("can not remove directory %s." % dir_name)

    logger.info("make dir %s" % dir_name)
    os.mkdir(dir_name)
    for i in file_name:
        fp.create_random_file(i, file_size)
        logger.info("move file %s to dir %s" % (i, dir_name))
        shutil.move(i, dir_name)

def p_u_d(dir_name, file_size, bucket_name, file_number, file_name, datanode_number, restartyts3, restartytsn):
    prepare_dir(dir_name, file_size, file_name)
    upload_dir(bucket_name, dir_name, file_number, datanode_number, restartyts3, restartytsn)
    time.sleep(int(30))
    download_dir_file(bucket_name, dir_name, file_name, file_size)

def p_u_d_docker(dir_name, file_size, bucket_name, file_number, file_name, datanode_number, restartyts3, restartytsn):
    prepare_dir(dir_name, file_size, file_name)
    upload_dir_docker(bucket_name, dir_name, file_number, datanode_number, restartyts3, restartytsn)
    time.sleep(int(30))
    download_dir_file(bucket_name, dir_name, file_name, file_size)


def p_u_d_container(dir_name, file_size, bucket_name, file_number, file_name, datanode_number, restartyts3, restartytsn):
    prepare_dir(dir_name, file_size, file_name)
    upload_dir_container(bucket_name, dir_name, file_number, datanode_number, restartyts3, restartytsn)
    time.sleep(int(30))
    download_dir_file(bucket_name, dir_name, file_name, file_size)


def p_u_d_report(report_file, dir_name, file_size, bucket_name, file_number, file_name, datanode_number, restartyts3, restartytsn):
    prepare_dir(dir_name, file_size, file_name)
    upload_dir_report(report_file, bucket_name, dir_name, file_number, datanode_number, restartyts3, restartytsn)
    time.sleep(int(30))
    download_dir_file_report(report_file, bucket_name, dir_name, file_name, file_size)

def get_config():
    global datanode_number, max_times, file_size
    list_config = []
    config_dir_path = os.getcwd()
    print "config path: %s" % config_dir_path

    config_file_list = []
    config_file_num = 0
    list = os.listdir(config_dir_path)
    for file in list:
        if file.startswith("config_"):
            config_file_num += 1
            config_file_list.append(os.path.join(config_dir_path, file))

    logger.info("config_file_list: %s" % config_file_list)

    if config_file_num == 0:
        logger.info("there is no config file, exit.")
        return
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

    for config_file in config_file_list:
        # open config file to read bucket info, file name, file size and file number
        logger.info("open config file %s..." % config_file)
        with open(config_file, "r") as config:
            for line in config:
                content = line.strip("\n").split("=")
                if content[0] == "bucket":
                    bucket_name = content[1]
                elif content[0] == "size":
                    file_size = content[1]
                elif content[0] == "number":
                    file_number = content[1]
                elif content[0] == "times":
                    max_times = content[1]
                elif content[0] == "datanode":
                    datanode_number = content[1]
                #elif content[0] == "transfer_way":
                #    transfer_way = content[1]
                #elif content[0] == "access_key":
                #    access_key = content[1]
                #elif content[0] == "secret_key":
                #    secret_key = content[1]

        dic_config_param = {}
        dic_config_param["configfilename"] = config_file
        dic_config_param["bucket_name"] = bucket_name
        dic_config_param["file_size"] = file_size
        dic_config_param["file_number"] = file_number
        dic_config_param["transfer_way"] = transfer_way
        dic_config_param["access_key"] = access_key
        dic_config_param["secret_key"] = secret_key

        if datanode_number != 0:
            dic_config_param["datanode_number"] = datanode_number
        if max_times != 0:
            dic_config_param["max_times"] = max_times
        list_config.append(dic_config_param)
    return list_config
