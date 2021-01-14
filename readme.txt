一. 配置和运行：
1.配置文件：config_stress_test.txt: 配置上传bucket，单个文件大小size，数量number，go版本s3安装的目录名称s3dir，注册的用户名userName和私钥privateKey，用于上传之前重启s3后注册使用。还需要配置发送邮件的用户名和密码：mail_user, mail_password。sender, receiver为邮件发送和接收地址。
2. /root/.s3cfg配置文件需要和config_stress_test.txt文件中的用户信息一致。
然后运行nohup python upload_ones3cmd_go.py &

如果配置为定时任务，需要如下配置：
二. 配置定时任务：查看crond定时服务是否开启，systemctl status crond，如果运行，就不用开启，如果未运行需要systemctl start crond开启定时服务。然后配置crontab，设置定时任务，如每天0点30运行压力测试

crontab -e
未配置邮件服务器配置为：0 20 * * * flock -xn /root/stress_test.lock -c 'python /root/upload_ones3cmd_go.py > /root/nohup.txt &'   //如配置为每天20：00定时运行



三.下载指定日期的bucket下面的所有文件: python download.py，配置文件config_download_test.txt: bucket_start: 开始日期， bucket_end: 结束日期