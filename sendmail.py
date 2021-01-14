#!/usr/bin/env python
# -*- coding: utf-8 -*-

import smtplib
from email.mime.text import MIMEText

mail_host = 'smtp.exmail.qq.com'
mail_user = 'gengwenjuan@yottachain.io'
mail_password = 'Wendy_216'
sender = 'gengwenjuan@yottachain.io'
receiver = ['gengwenjuan@yottachain.io']

content = "sff" + " 12222222"
message = MIMEText(content, 'plain', 'utf-8')
message['Subject'] = 'go s3 speed'
message['From'] = sender
message['To'] = receiver[0]


try:
    smtpObj = smtplib.SMTP()
    smtpObj.connect(mail_host, 25)
    smtpObj.login(mail_user, mail_password)
    smtpObj.sendmail(message['From'], message['To'], message.as_string())

    smtpObj.quit()
    print "send success"
except smtplib.SMTPException as e:
    print "send error: %s" % e