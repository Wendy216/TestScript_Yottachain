Note:
1.autotest.sh包括所有的测试用例脚本， log.txt是运行时产生的log文件，report.txt为报告文件，version.txt为版本信息
2.config_开头的配置文件为上传文件的信息，config_fixed开头的为固定大小，config_rand开头的为随机大小，config_dir开头的为丢包和延时用例信息
3.key.txt文件里面包含用户配置信息：transfer_way=s3cmd代表使用s3cmd命令， transfer_way=s3代表使用s3的api接口，s3 api接口方式需要在配置文件加入access_key, secret_key信息
