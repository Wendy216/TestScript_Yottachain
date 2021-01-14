#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import requests
import smtplib
from email.mime.text import MIMEText
import datetime

file_number = 10
file_size = 10
log_file = 'log.txt'
s3cmd_home = "/usr/bin/s3cmd"
report_file = "report.txt"
l_c = Lock()
yts3_log = []
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line: %(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

url_prefix = 'http://localhost:8080/api/v1/'

mail_host = 'smtp.exmail.qq.com'
#mail_user = 'gengwenjuan@yottachain.io'
#mail_password = 'Wendy_216'
#sender = 'gengwenjuan@yottachain.io'
#receiver = ['gengwenjuan@yottachain.io']

def send_main():
    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)
        smtpObj.login(mail_user, mail_password)
        smtpObj.sendMail(sender, receiver, message.as_string())

        smtpObj.quit()
        print "send success"
    except smtplib.SMTPException as e:
        print "send error: %s" % e

# create file with random content and fixed size
def create_file(file_name, size):
    if os.path.exists(file_name):
        os.remove(file_name)
    with open(file_name, 'wb') as f:
        f.write(os.urandom(size))
        logger.info("create file %s" % file_name)


# check whether the bucket is exist, if not exist, make the parameter bucket
def check_bucket(bucket):
    find_bucket = False
    l_c.acquire()
    #logger.info("call s3cmd to list all bucket.")
    ret, output = commands.getstatusoutput("/usr/bin/s3cmd --no-ssl ls")
    if ret != 0:
        logging.error("s3cmd ls error.")
        sys.exit(1)

    if output == "":
        find_bucket = False
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            result = line.split("//")[-1]
            if bucket == result:
                find_bucket = True
                break

    if not find_bucket:
        ret, output = commands.getstatusoutput("/usr/bin/s3cmd --no-ssl mb s3://%s" % bucket)
        #print ret, output
        if ret != 0:
            logging.error("make bucket %s error." % bucket)
            sys.exit(1)

    l_c.release()
    return True


# upload file to the bucket specified in config file
def upload_dir(bucket, dir_name):
    check = False
    if check_bucket(bucket):
        logger.info("call s3cmd to upload dir %s to bucket %s." % (dir_name, bucket))
        time_start = time.time()
        ret, output = commands.getstatusoutput("/usr/bin/s3cmd --no-ssl put %s/* s3://%s" % (dir_name, bucket))
        if ret != 0:
            logger.error("upload dir %s error!", dir_name)
            shutil.rmtree(dir_name)
            logger.info("delete dir %s" % dir_name)
            return
        time_end = time.time()
        total_time = time_end - time_start
        logger.info("upload dir %s totally cost %f" % (dir_name, total_time))
    print "s3md finished upload, delete dir %s" % dir_name
    shutil.rmtree(dir_name)
    logger.info("delete dir %s" % dir_name)


def check_log_uploaded(dir_name, file_number):
    ret, output = commands.getstatusoutput("cat %s/GO_YTS3_Linux/log/log | grep 'CreateObject' | wc -l" % dir_name)
    if ret != 0:
        logger.error("fail to run cat %s/GO_YTS3_Linux/log/log | grep 'CreateObject' | wc -l" % dir_name)
        sys.exit(1)
    while int(output) != file_number:
        ret, output = commands.getstatusoutput("cat %s/GO_YTS3_Linux/log/log | grep 'CreateObject' | wc -l" % dir_name)
        if ret != 0:
            logger.error("fail to run cat %s/GO_YTS3_Linux/log/log | grep 'CreateObject' | wc -l" % dir_name)
            sys.exit(1)


def check_bucket_uploaded(bucket_name, dir_name, file_number):
    ret, output = commands.getstatusoutput("s3cmd --no-ssl ls s3://%s/%s | wc -l" % (bucket_name, dir_name))
    if ret != 0:
        logger.error("error to run s3cmd ls s3://%s/%s | wc -l" % (bucket_name, dir_name))
        sys.exit(1)
    if "ERROR" in output:
        logger.error("ERROR in running s3cmd ls s3://%s/%s | wc -l" % (bucket_name, dir_name))
        sys.exit(1)
    if int(output) == file_number:
        logger.info("%d is equal to %d" % (int(output), file_number))


def prepare_dir(dir_name, file_number, file_size):
    if os.path.exists(dir_name):
        ret, output = commands.getstatusoutput("rm -rfv %s" % dir_name)
        if ret != 0:
            logger.error("can not remove directory %s." % dir_name)
            sys.exit(1)

    logger.info("make dir %s" % dir_name)
    os.mkdir(dir_name)
    for i in range(int(file_number)):
        file_name = dir_name + ".txt%d" % i
        create_file(file_name, file_size)

        logger.info("move file %s to dir %s" % (file_name, dir_name))
        shutil.move(file_name, dir_name)


def get_unupload_file(file_number):
    uploaded_number = 0
    ret, output = commands.getstatusoutput("ls /mnt/yts3/log/log* | wc -l")
    if ret != 0:
        print "fail to get log* number."
        unupload = 6
    else:
        if int(output) > 1:
            ret, output1 = commands.getstatusoutput("ls /mnt/yts3/log/log*")
            if ret != 0:
                print "fail to get log file name."
                unupload = 6
            for i in output1.split("\n"):
                #print "i: %s" % i
                yts3_log.append(i)
                ret, output2 = commands.getstatusoutput("cat %s | grep 'uploaded successfully' | wc -l" % i)
                uploaded_number += int(output2)
            print "uploaded_number: %d" % uploaded_number
            unupload = int(file_number) - uploaded_number
            #print "unupload: %d" % unupload
        else:
            ret, output = commands.getstatusoutput("cat /mnt/yts3/log/log | grep 'uploaded successfully' | wc -l")
            if ret != 0:
                print "fail to get file number of uploaded successfully.\n"
                unupload = 6
            else:
                unupload = int(file_number) - int(output)
    return unupload


def insertuser(userName, privateKey):
    url = "%sinsertuser" % url_prefix
    data = {
    "userName": userName,
    "privateKey": privateKey
    }

    try:
        response = requests.post(url=url, data=data)
        return response.json()
    except ValueError as e:
        print "ValueError"
        print "Fail to register user %s. Please check the privateKey!" % userName
        logger.error("Fail to register user %s. Please check the privateKey!" % userName)
        sys.exit(1)


#delete s3 log file and register user
def pre_work(s3dir, userName, privateKey):
    ret, output = commands.getstatusoutput("rm -f %s/GO_YTS3_Linux/log/*" % s3dir)
    if ret != 0:
        logger.error("fail to delete %s/GO_YTS3_Linux/log/*" % s3dir)
        sys.exit(1)
    ret, output = commands.getstatusoutput("systemctl restart yts3.service")
    if ret != 0:
        logger.error("fail to restart yts3")
        sys.exit(2)
    time.sleep(3)
    insertuser(userName, privateKey)


if __name__ == '__main__':

    logger.info("loading config...")

    config_dir_path = os.getcwd()
    print "config path: %s" % config_dir_path

    config_file_list = []
    config_file_num = 0
    list = os.listdir(config_dir_path)
    for file in list:
        #print file
        if file.startswith("config_stress"):
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
                if content[0] == "bucket":
                    bucket_name = content[1]
                elif content[0] == "size":
                    file_size = content[1]
                elif content[0] == "number":
                    file_number = content[1]
                elif content[0] == "s3dir":
                    s3dir = content[1]
                elif content[0] == "userName":
                    userName = content[1].strip()
                elif content[0] == "privateKey":
                    privateKey = content[1].strip()
                elif content[0] == "s3ip":
                    s3ip = content[1]
                elif content[0] == "sender":
                    sender = content[1].strip()
                elif content[0] == "receiver":
                    receiver_all = content[1].strip()
                elif content[0] == "mail_user":
                    mail_user = content[1]
                elif content[0] == "mail_password":
                    mail_password = content[1]

        print "bucket_name: %s" % bucket_name
        print "file_size: %s" % file_size
        print "file_number: %s" % file_number
        print "s3dir: %s" % s3dir
        print "userName: %s" % userName
        print "privateKey: %s" % privateKey
        print "sender: %s" % sender
        print "receiver: %s" % receiver_all
        print "mail_user: %s" % mail_user
        print "mail_password: %s" % mail_password

        pre_work(s3dir, userName, privateKey)
        bucket_name = datetime.date.today()

        uuid_str = uuid.uuid4().hex[0:6]
        tmp_file_name = 'tmpfile_%s_%s' % (bucket_name, uuid_str)
        dir_name = tmp_file_name

        t = threading.Thread(target=prepare_dir, args=(dir_name, int(file_number), int(file_size)))
        t.start()
        t.join()

        start_time = time.time()
        print "start_time: %s" % time.strftime("%Y-%m-%d: %H:%M:%S")

        upload_dir(str(bucket_name), dir_name)
        check_log_uploaded(s3dir, int(file_number))
        check_bucket_uploaded(bucket_name, dir_name, int(file_number))

        end_time = time.time()
        print "end_time: %s" % time.strftime("%Y-%m-%d: %H:%M:%S")

        total_time = end_time - start_time
        #print "total_time: %s" % total_time

        total_size = int(file_number) * int(file_size)
        speed = total_size / 1024 / 1024 /  total_time
        logger.info("upload average speed is %.2f MB/s" % speed)

        mail_content = "upload average speed: " + str('{:.2f}'.format(speed)) + "MB/s from server: " + s3ip

        message = MIMEText(mail_content, 'plain', 'utf-8')
        message['Subject'] = 'go s3 upload speed from server %s' % s3ip
        message['From'] = sender
        #print "message['From']: %s" % message['From']
        receivers = receiver_all.split('zzz')
        message['To'] = ','.join(receivers)
        #print "message['To']: %s" % message['To']

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host, 25)
            smtpObj.login(sender, mail_password)
            smtpObj.sendmail(sender, message['To'].split(','), message.as_string())

            smtpObj.quit()
            print "send success from s3: %s" % s3ip
        except smtplib.SMTPException as e:
            print "send error: %s" % e