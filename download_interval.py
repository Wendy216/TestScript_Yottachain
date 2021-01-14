#/use/bin/env python
#-*- coding:utf-8 -*-

import os
import sys
import datetime
import time
import commands
import logging
import threading
import smtplib
from email.mime.text import MIMEText

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
interval = 2

total_speed = 0
bucket_success = 0
mail_host = 'smtp.exmail.qq.com'

def get_s3cmd_path():
    global s3cmd_home
    ret, output = commands.getstatusoutput("which s3cmd")
    if ret != 0:
        logger.error("fail to get s3cmd path.")
        sys.exit(1)
    s3cmd_home = output.strip("\n")
    return s3cmd_home


def check_upload(bucket_name):
    total_size = 0
    s3cmd_home = get_s3cmd_path()
    ret, output = commands.getstatusoutput("%s --no-ssl ls s3://%s" % (s3cmd_home, bucket_name))
    if ret != 0:
        logging.error("s3cmd ls s3://%s error." % bucket)
        sys.exit(1)


def download_file(bucket_name):
    global total_speed
    global bucket_success
    total_size = 0
    s3cmd_home = get_s3cmd_path()
    check_upload(bucket_name)
    start_time = time.time()
    ret, output = commands.getstatusoutput("%s --no-ssl get s3://%s/* --force" % (s3cmd_home, bucket_name))
    if ret != 0:
        logger.error("Fail to download files in bucket %s and output is %s" % (bucket_name, output))
        sys.exit(2)
    bucket_success += 1
    logger.info("Download bucket %s successfully." % bucket_name)
    end_time = time.time()
    total_time = end_time - start_time
    print "total_time: %d" % total_time
    ret, output = commands.getstatusoutput("%s --no-ssl ls s3://%s" % (s3cmd_home, bucket_name))
    if ret != 0:
        logging.error("s3cmd ls s3://%s error." % bucket_name)
        sys.exit(1)
    if output == " ":
        logger.info("bucket %s is empty." % bucket_name)
        sys.exit(3)
    else:
        Alloutput = output.split("\n")
        for line in Alloutput:
            size = line.split()[-2]
            total_size += int(size)
    logger.info("bucket %s total_size: %d" % (bucket_name, total_size))
    speed = total_size / 1024 / 1024 / total_time
    logger.info("download bucket %s average speed is %.2f MB/s" % (bucket_name, speed))
    total_speed += float(speed)
    logger.info("total speed is %.2f MB/s" % total_speed)


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

    for config_file in config_file_list_split:
        # open config file to read bucket info, file name, file size and file number
        logger.info("open config file %s..." % config_file)
        with open(config_file, "r") as config:
            for line in config:
                content = line.strip("\n").split("=")
                if content[0] == "interval":
                    interval = content[1]
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

        print "interval: %d" % int(interval)

        get_bucket_name()
        #current date
        current = datetime.datetime.now()

        offset = datetime.timedelta(days=-int(interval))
        offset_date = (current + offset).strftime("%Y-%m-%d")

        current_date = current.strftime("%Y-%m-%d")

        threads = []
        times = 0

        for date in dateRange(offset_date, current_date):
            print date
            if date in bucket_list:
                times += 1
                t = threading.Thread(target=download_file, args=(date,))
                t.start()
                threads.append(t)

        for th in threads:
            th.join()

        average_speed = total_speed / bucket_success
        logger.info("download average speed is %.2f MB/s" % average_speed)

        mail_content = "download average speed: " + str('{:.2f}'.format(average_speed)) + "MB/s from server: " + s3ip

        message = MIMEText(mail_content, 'plain', 'utf-8')
        message['Subject'] = 'go s3 download speed from server %s' % s3ip
        message['From'] = sender
        # print "message['From']: %s" % message['From']
        receivers = receiver_all.split('zzz')
        message['To'] = ','.join(receivers)
        # print "message['To']: %s" % message['To']

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host, 25)
            smtpObj.login(sender, mail_password)
            smtpObj.sendmail(sender, message['To'].split(','), message.as_string())

            smtpObj.quit()
            print "send success from s3: %s" % s3ip
        except smtplib.SMTPException as e:
            print "send error: %s" % e