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
import module_variable
import fileoperation_s3 as fps3

file_number = 10
file_size = 10
log_file = 'log.txt'
#s3cmd_home = "/usr/bin/s3cmd"
report_file = "report.txt"
version_file="version.txt"
l_c = Lock()
fixed_size = 1024 * 1024 * 5   # 5M

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3cmd_home = module_variable.get_s3cmd_path()

def append_report(report_file, content):
    try:
        with open(report_file, 'a') as f:
            f.writelines(content)
    except IOError:
        logger.error("fail to open report file %s." % report_file)


def append_version(report_file, version_file):
    append_report(report_file, "\n")
    try:
        with open(version_file, 'r') as f:
            for line in f:
                append_report(report_file, line)
    except IOError:
        logger.error("fail to open version file %s." % version_file)


def write_chunck(file_name, file_size):
    global fixed_size
    pos = fixed_size
    with open(file_name, 'wb') as f:
        while pos < file_size:
            #f.seek(fixed_size)
            f.write(os.urandom(fixed_size))
            #logger.info("write %s" % fixed_size)
            pos = os.path.getsize(file_name)
            #logger.info("pos: %s" % pos)
            if file_size - pos < fixed_size:
                break
        #f.seek(file_size - pos)
        f.write(os.urandom(file_size - pos))


# create file with random content and fixed size
def create_random_file(file_name, file_size):
    global fixed_size
    logger.info("fixed size: %d" % fixed_size)
    logger.info("create file %s file size %d" % (file_name, file_size))
    if os.path.isfile(file_name):
        os.remove(file_name)
    if file_size > fixed_size:
        write_chunck(file_name, file_size)
    else:
        with open(file_name, 'wb') as f:
            #f.seek(file_size)
            f.write(os.urandom(file_size))
    if os.path.getsize(file_name) != file_size:
        logger.error("create file %s error" % file_name)
        sys.exit(1)


# check whether the bucket is exist, if not exist, make the parameter bucket
def check_bucket(bucket):
    find_bucket = False
    l_c.acquire()
    ret, output = commands.getstatusoutput("%s --no-ssl ls" % s3cmd_home)
    if ret != 0:
        logging.error("s3cmd ls error.")

    if output == "":
        find_bucket = False
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            result = line.split("//")[-1]
            if bucket == result:
                find_bucket = True
                break

    if find_bucket == False:
        ret, output = commands.getstatusoutput("%s --no-ssl mb s3://%s" % (s3cmd_home, bucket))
        if ret != 0:
            logging.error("make bucket %s error." % bucket)

    l_c.release()
    return True


def check_file_if_exist(file):
    exist = False
    if os.path.isfile(file):
        exist = True
    return exist


# before download check s3cmd ls s3://bucket_name/file_name whether the file has been uploaded
def check_upload(bucket, file_name, size):
    find_upload = False

    ret, output = commands.getstatusoutput("%s --no-ssl ls s3://%s/%s" % (s3cmd_home, bucket, file_name))
    if ret != 0:
        logging.error("s3cmd ls s3://%s error." % bucket)
        return find_upload

    if output == "":
        find_upload = False
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            result = line.split("/")[-1]
            if file_name == result:
                find_upload = True
                logger.info("upload %s=%s successfully." % (file_name, size))
                break
    return find_upload


# upload file to the bucket specified in config file
def upload_file_report(report_file, bucket, file_name, size):
    check = False
    if check_bucket(bucket) and check_file_if_exist(file_name):
        logger.info("call s3cmd to upload file %s to bucket %s." % (file_name, bucket))
        time_start = time.time()
        ret, output = commands.getstatusoutput("%s --no-ssl put %s s3://%s" % (s3cmd_home, file_name, bucket))
        if ret != 0:
            logger.error("upload file %s error!", file_name)
            return
        else:
            time_end = time.time()
            total_time = time_end - time_start
            logger.info("upload file %s totally cost %f" % (file_name, total_time))

    if size <= 105760:
        time_wait = 300
    elif 105760 < size < 10576000:
        time_wait = 600
    elif size >= 10576000:
        time_wait = 1800
    time_start = time.time()
    while not check:
        time_end = time.time()
        if time_end - time_start > time_wait:
            break
        check = check_upload(bucket, file_name, size)
    if check == True:
        append_report(report_file, "[%s] - upload_file_%s_size=%d_Pass\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
    else:
        append_report(report_file, "[%s] - upload_file_%s_size=%d_Fail\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))


def only_upload(bucket, file_name, size, transfer_way, access_key, secret_key):
    check = False
    if check_bucket(bucket) and check_file_if_exist(file_name):
        if transfer_way == "s3cmd":
            logger.info("call s3cmd to upload file %s to bucket %s." % (file_name, bucket))
            time_start = time.time()
            ret, output = commands.getstatusoutput("%s --no-ssl put %s s3://%s" % (s3cmd_home, file_name, bucket))
            if ret != 0:
                logger.error("upload file %s error!", file_name)
                return
            else:
                time_end = time.time()
                total_time = time_end - time_start
                logger.info("upload file %s totally cost %f" % (file_name, total_time))
        elif transfer_way == "s3":
            logger.info("call s3 interface to upload file %s to bucket %s." % (file_name, bucket))
            time_start = time.time()
            s3 = fps3.S3ud(access_key, secret_key)
            s3.upload_file(file_name, bucket, file_name)
            time_end = time.time()
            total_time = time_end - time_start
            logger.info("upload file %s totally cost %f" % (file_name, total_time))
        else:
            logger.error("transfer way error!")
            sys.exit(1)


# upload file to the bucket specified in config file
def upload_file(bucket, file_name, size, transfer_way, access_key, secret_key):
    check = False
    if check_bucket(bucket) and check_file_if_exist(file_name):
        if transfer_way == "s3cmd" :
            logger.info("call s3cmd to upload file %s to bucket %s." % (file_name, bucket))
            time_start = time.time()
            ret, output = commands.getstatusoutput("%s --no-ssl put %s s3://%s" % (s3cmd_home, file_name, bucket))
            if ret != 0:
                logger.error("upload file %s error!", file_name)
                return
            else:
                time_end = time.time()
                total_time = time_end - time_start
                logger.info("upload file %s totally cost %f" % (file_name, total_time))
        elif transfer_way == "s3":
            logger.info("call s3 interface to upload file %s to bucket %s." % (file_name, bucket))
            time_start = time.time()
            s3 = fps3.S3ud(access_key, secret_key)
            s3.upload_file(file_name, bucket, file_name)
            time_end = time.time()
            total_time = time_end - time_start
            logger.info("upload file %s totally cost %f" % (file_name, total_time))
        else:
            logger.error("transfer way error!")
            sys.exit(1)

    if size <= 105760:
        time_wait = 300
    elif 105760 < size < 10576000:
        time_wait = 600
    elif size >= 10576000:
        time_wait = 1800
    time_start = time.time()
    while not check:
        time_end = time.time()
        if time_end - time_start > time_wait:
            break
        time.sleep(10)
        check = check_upload(bucket, file_name, size)

    if check == True:
        append_report(report_file, "[%s] - upload_file_%s_size=%d_Pass\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
    else:
        append_report(report_file, "[%s] - upload_file_%s_size=%d_Fail\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))


#  download file from bucket and check whether the md5sum is same
def download_file(bucket, file_name, size, transfer_way, access_key, secret_key):
    upload_file_size = os.path.getsize(file_name)
    download_file_name = file_name + ".down"
    time_start = time.time()
    find = check_upload(bucket, file_name, size)
    time.sleep(5)
    if not find:
        logger.error("the file %s is not exist in bucket %s" % (file_name, bucket))
        append_report(report_file, "[%s] - download_file_%s_size=%d_is not exist.\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        return
    else:
        if transfer_way == "s3cmd":
            ret, output = commands.getstatusoutput(
                "%s --no-ssl get s3://%s/%s %s" % (s3cmd_home, bucket, file_name, download_file_name))
            if ret != 0:
                append_report(report_file, "[%s] - download_file_%s_size=%d_Fail\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
                logger.error("download_file_%s_output_%s" % (file_name, output))
                logger.error("download_file_%s_size=%d_Fail\n" % (file_name, size))
                return
        elif transfer_way == "s3" :
            s3 = fps3.S3ud(access_key, secret_key)
            s3.download(bucket_name=bucket, file_name=file_name, d_file_name=download_file_name)
        else:
            logger.error("transfer way error!")
            return

    time_end = time.time()
    total_time = time_end - time_start

    if not os.path.exists(download_file_name):
        logger.info("the download file %s is not exist." % file_name)
        append_report(report_file, "[%s] - download_file_%s_size=%d_Fail.\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        return 
    download_file_size = os.path.getsize(file_name + ".down")
    if upload_file_size == download_file_size:
        append_report(report_file, "[%s] - download_file_%s_size=%d_Pass\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        logger.info("download file %s successfully." % file_name)
    else:
        append_report(report_file, "[%s] - download_file_%s_size=%d_Fail\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        return

    ret, output = commands.getstatusoutput("diff %s %s.down" % (file_name, file_name))
    if ret != 0:
        logger.error("files %s and %s.down are not same." % (file_name, file_name))
        append_report(report_file, "[%s] - compare_download_file %s and %s.down Fail\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, file_name))
        return
    else:
        logger.info("files %s and %s.down are same." % (file_name, file_name))
        append_report(report_file, "[%s] - compare_download_file %s and %s.down Pass\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, file_name))

        ret, output = commands.getstatusoutput("rm -f %s %s.down" % (file_name, file_name))
        if ret == 0:
            logger.info("remove file %s %s.down" % (file_name, file_name))


def download_file_report(report_file, bucket, file_name, size):
    upload_file_size = os.path.getsize(file_name)
    download_file_name = file_name + ".down"
    time_start = time.time()
    find = check_upload(bucket, file_name, size)
    if not find:
        logger.error("the file %s is not exist in bucket %s" % (file_name, bucket))
        append_report(report_file, "[%s] - download_file_%s_size=%d_is not exist.\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        return
    else:
        ret, output = commands.getstatusoutput(
            "%s --no-ssl get s3://%s/%s %s" % (s3cmd_home, bucket, file_name, download_file_name))
        if ret != 0:
            append_report(report_file, "[%s] - download_file_%s_size=%d_Fail\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
            logger.error("download_file_%s_size=%d_Fail\n" % (file_name, size))
            return
    time_end = time.time()
    total_time = time_end - time_start

    if not os.path.exists(download_file_name):
        logger.info("the download file %s is not exist.")
        append_report(report_file, "[%s] - download_file_%s_size=%d_is not exist.\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        return
    download_file_size = os.path.getsize(file_name + ".down")
    if upload_file_size == download_file_size:
        append_report(report_file, "[%s] - download_file_%s_size=%d_Pass\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        logger.info("download file %s successfully." % file_name)
    else:
        append_report(report_file, "[%s] - download_file_%s_size=%d_Fail\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), file_name, size))
        return

    ret, output = commands.getstatusoutput("diff %s %s.down" % (file_name, file_name))
    if ret != 0:
        logger.error("files %s and %s.down are not same." % (file_name, file_name))
        append_report(report_file, "[%s] - compare_download_file %s and %s.down Fail\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, file_name))
        return
    else:
        logger.info("files %s and %s.down are same." % (file_name, file_name))
        append_report(report_file, "[%s] - compare_download_file %s and %s.down Pass\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), file_name, file_name))

        ret, output = commands.getstatusoutput("rm -f %s %s.down" % (file_name, file_name))
        if ret == 0:
            logger.info("remove file %s %s.down" % (file_name, file_name))

def c_u_d(file_name, size, bucket_name, transfer_way, access_key, secret_key):
    create_random_file(file_name, size)
    upload_file(bucket_name, file_name, size, transfer_way, access_key, secret_key)
    time.sleep(int(30))
    download_file(bucket_name, file_name, size, transfer_way, access_key, secret_key)

def c_u_d_report(report_file, file_name, size, bucket_name):
    create_random_file(file_name, size)
    upload_file_report(report_file, bucket_name, file_name, size)
    time.sleep(int(30))
    download_file_report(report_file, bucket_name, file_name, size)

def u_d(bucket_name, file_name, size, transfer_way, access_key, secret_key):
    upload_file(bucket_name, file_name, size, transfer_way, access_key, secret_key)
    time.sleep(int(30))
    download_file(bucket_name, file_name, size, transfer_way, access_key, secret_key)


def u_d_report(report_file, bucket_name, file_name, size):
    upload_file_report(report_file, bucket_name, file_name, size)
    time.sleep(int(30))
    download_file_report(report_file, bucket_name, file_name, size)


def c_u(bucket_name, file_name, size, transfer_way, access_key, secret_key):
    create_random_file(file_name, size)
    upload_file(bucket_name, file_name, size, transfer_way, access_key, secret_key)

def c_only_u(bucket_name, file_name, size, transfer_way, access_key, secret_key):
    create_random_file(file_name, size)
    only_upload(bucket_name, file_name, size, transfer_way, access_key, secret_key)


def restart_yts3():
    append_report(report_file, "[%s] - begin to restart yts3.\n" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    ret, output = commands.getstatusoutput("service yts3 restart")
    if ret != 0:
        logger.error("restart yts3 error.")
    logger.info("restart yts3 successfully.")
    append_report(report_file, "[%s] - restart yts3 successfully.\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S")))


def restart_yts3_docker():
    append_report(report_file, "[%s] - begin to restart yts3.\n" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    running_s3node = 0
    s3node_id = []
    ret, output = commands.getstatusoutput("docker ps -a --filter status=running | grep s3server | awk '{print $1}'")
    if ret != 0:
        logger.error("get running s3server error.")
        # sys.exit(1)

    if output == "":
        logger.warn("no s3server running")
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            running_s3node += 1
            s3node_id.append(line)

    logger.info("running_s3node: %d" % running_s3node)
    logger.info("s3node_id : %s" % s3node_id)

    for line in s3node_id:
        ret, output = commands.getstatusoutput("docker exec -itd %s /bin/bash -c '/yts3/bin/yts3.sh restart'" % line)
        time.sleep(2)
        if ret != 0:
            logger.error("restart yts3 %s error." % line)
        logger.info("restart yts3 %s successfully." % line)


def restart_yts3_container():
    append_report(report_file, "[%s] - begin to restart yts3.\n" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    ret, output = commands.getstatusoutput("/yts3/bin/yts3.sh restart")
    if ret != 0:
        logger.error("restart yts3 error.")
    logger.info("restart yts3 successfully.")
    append_report(report_file, "[%s] - restart yts3 successfully.\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S")))


def restart_ytsn():
    running_snnode = 0
    snnode_id = []
    ret, output = commands.getstatusoutput("docker ps -a --filter status=running | grep sndocker | awk '{print $1}'")
    if ret != 0:
        logger.error("get running snnode error.")
        #sys.exit(1)

    if output == "":
        logger.warn("no snnode running")
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            running_snnode += 1
            snnode_id.append(line)

    logger.info("running_snnode: %d" % running_snnode)
    logger.info("snnode_id : %s" % snnode_id)

    for line in snnode_id:
        ret, output = commands.getstatusoutput("docker exec -itd %s /bin/bash -c '/app/ytsn/bin/ytsnd.sh restart'" % line)
        time.sleep(2)
        if ret != 0:
            logger.error("restart ytsn %s error." % line)
            #sys.exit(1)
        logger.info("restart ytsn %s successfully." % line)


def check_mongo():
    running_snnode = 0
    snnode_id = []
    mongo_result = []
    running_dnnode = 0
    dnnode_id = []

    ret, output = commands.getstatusoutput("docker ps -a --filter status=running | grep dndocker | wc -l")
    if ret != 0:
        logger.error("get running dnnode error.")
        return

    if output == "":
        logger.warn("no dnnode running")
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            running_dnnode += 1
            dnnode_id.append(line)

    logger.info("running_dnnode: %d" % running_dnnode)
    logger.info("dnnode_id : %s" % dnnode_id)

    ret, output = commands.getstatusoutput("docker ps -a --filter status=running | grep sndocker | awk '{print $1}'")
    if ret != 0:
        logger.error("get running snnode error.")
        return

    if output == "":
        logger.warn("no snnode running")
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            running_snnode += 1
            snnode_id.append(line)

    logger.info("running_snnode: %d" % running_snnode)
    logger.info("snnode_id : %s" % snnode_id)


    for line in range(running_snnode):
        ret, output = commands.getstatusoutput("/app/mongodb-4.0.10/bin/mongo --port 27017 --eval 'db.Node.find({'status':1, 'valid':1, 'timestamp': {$gt: NumberInt(new Date().getTime()/1000)-180}, 'rx': {$gt: NumberLong(0)}, 'tx': {$gt: NumberLong(0)}}).count()' yotta%d" % line)
        if ret != 0:
            append_report(report_file, "[%s] - check database yotta%d fail.\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), line))
            logger.error("check database yotta%d error" % line)
        else:
            for item in output.split("\n"):
                mongo_result.append(item)
            print "mongo_result: %d" % int(mongo_result[-1])
            if int(mongo_result[-1]) == 0:
                logger.info("database yotta%d rx, tx item is zero" % line)
                append_report(report_file,
                              "[%s] - database yotta%d rx,tx item is zero.\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), line))
            elif int(mongo_result[-1]) > 0 and int(mongo_result[-1]) == running_dnnode:
                logger.info("database yotta%d rx,tx item %d is greater than zero and equal to dnnode number %d" % (line, int(mongo_result[-1]), int(running_dnnode)))
                append_report(report_file,
                              "[%s] - database yotta%d rx,tx item %d is greater than zero and equal to dnnode number %d.\n" % (
                              time.strftime("%Y-%m-%d %H:%M:%S"), line, int(mongo_result[-1]), int(running_dnnode)))


def close_partial_datanode(datanode_number):
    running_number = 0
    datanode_id = []
    append_report(report_file, "[%s] - begin to stop datanode number=%d.\n" % (
        time.strftime("%Y-%m-%d %H:%M:%S"), int(datanode_number)))

    ret, output = commands.getstatusoutput("docker ps -a --filter status=running | grep dndocker | awk '{print $1}'")
    if ret != 0:
        logger.error("get running datanode error.")
        #sys.exit(1)

    if output == "":
        logger.warn("no datanode running")
        #sys.exit(1)
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            running_number += 1
            datanode_id.append(line)
    logger.info("to be close datanode_number: %d" % datanode_number)
    print "to be close datanode_number: %d" % datanode_number
    logger.info("running_number: %d" % running_number)
    logger.info("datanode_id: %s" % datanode_id)
    if int(datanode_number) < running_number:
        for i in range(int(datanode_number)):
            logger.info("start to stop datanode %s ..." % datanode_id[i])
            print "start to stop datanode %s ..." % datanode_id[i]
            ret, output = commands.getstatusoutput("docker stop %s" % datanode_id[i])
            time.sleep(5)
            if ret != 0:
                logger.error("stop datanode %s error." % datanode_id[i])
                print "stop datanode %s successfully." % datanode_id[i]
                append_report(report_file, "[%s] - stop datanode %s fail.\n" % (
                    time.strftime("%Y-%m-%d %H:%M:%S"), datanode_id[i]))
            logger.info("stop datanode %s successfully." % datanode_id[i])
            print "stop datanode %s successfully." % datanode_id[i]
            append_report(report_file, "[%s] - stop datanode %s successfully.\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S"), datanode_id[i]))
    elif datanode_number >= running_number:
        logger.error("to be closed datanode is more than running datanode, stop close datanode.")
        print "to be closed datanode is more than running datanode, stop close datanode."
        append_report(report_file, "[%s] - to be closed datanode is more than running datanode, stop close datanode.\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S")))


def lost_packet(dev, loss_ration):
    ret, output = commands.getstatusoutput("tc qdisc add dev %s root netem loss %s" % (dev, loss_ration))
    if ret != 0:
        logger.error("error to set lost packet on %s" % dev)
        result = output.strip("\n")
        if "File exists" in result:
            logger.info("need to delete set on dev %s" % dev)
            ret1, output2 = commands.getstatusoutput("tc qdisc del dev %s root" % dev)
            if ret1 != 0:
                logger.error("error to delete dev set on %s" % dev)
                #sys.exit(1)
            else:
                logger.info("tc qdisc del dev %s root successfuly." % dev)
                commands.getstatusoutput("tc qdisc add dev %s root netem loss %s" % (dev, loss_ration))
                logger.info("set lost packet on %s successfully." % dev)
    else:
        logger.info("set lost packet on %s %s successfully." % (dev, loss_ration))


def set_delay(dev):
    ret, output = commands.getstatusoutput("tc qdisc add dev %s root netem delay 100ms 10ms" % dev)
    if ret != 0:
        logger.error("error to set delay on %s" % dev)
        result = output.strip("\n")
        if "File exists" in result:
            logger.info("need to delete set on dev %s" % dev)
            ret1, output2 = commands.getstatusoutput("tc qdisc del dev %s root" % dev)
            if ret1 != 0:
                logger.error("error to delete dev set on %s" % dev)
                #sys.exit(1)
            else:
                logger.info("tc qdisc del dev %s successfully." % dev)
                commands.getstatusoutput("tc qdisc add dev %s root netem delay 100ms 10ms" % dev)
                logger.info("set delay on %s successfully." % dev)
    else:
        logger.info("set delay on %s successfully." % dev)
