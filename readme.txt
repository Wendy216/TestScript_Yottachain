һ. ���ú����У�
1.�����ļ���config_stress_test.txt: �����ϴ�bucket�������ļ���Сsize������number��go�汾s3��װ��Ŀ¼����s3dir��ע����û���userName��˽ԿprivateKey�������ϴ�֮ǰ����s3��ע��ʹ�á�����Ҫ���÷����ʼ����û��������룺mail_user, mail_password��sender, receiverΪ�ʼ����ͺͽ��յ�ַ��
2. /root/.s3cfg�����ļ���Ҫ��config_stress_test.txt�ļ��е��û���Ϣһ�¡�
Ȼ������nohup python upload_ones3cmd_go.py &

�������Ϊ��ʱ������Ҫ�������ã�
��. ���ö�ʱ���񣺲鿴crond��ʱ�����Ƿ�����systemctl status crond��������У��Ͳ��ÿ��������δ������Ҫsystemctl start crond������ʱ����Ȼ������crontab�����ö�ʱ������ÿ��0��30����ѹ������

crontab -e
δ�����ʼ�����������Ϊ��0 20 * * * flock -xn /root/stress_test.lock -c 'python /root/upload_ones3cmd_go.py > /root/nohup.txt &'   //������Ϊÿ��20��00��ʱ����



��.����ָ�����ڵ�bucket����������ļ�: python download.py�������ļ�config_download_test.txt: bucket_start: ��ʼ���ڣ� bucket_end: ��������