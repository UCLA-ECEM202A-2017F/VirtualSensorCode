import shutil
import time
import os
import subprocess

def check_virtual(interval: int, datapath: str, archive_path: str):
    start = time.time()
    master_num = 0
    cid = 1
    port_num = 8081
    port_idx = 0
    port_list = [8081, 8083, 8085]

    # os.environ['SPARK_IDENT_STRING'] = 'master{}'.format(master_num)
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh"])
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh"])
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh", "-h", "127.0.0.1", "-p", "{}".format(port_num)])
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh", "spark://127.0.0.1:{}".format(port_num)])
    # subprocess.Popen(["spark-submit", "--conf", "spark.cores.max=1", \
    # "--master", "spark://127.0.0.1:{}".format(port_num), \
    # "--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1", \
    # "default_session.py", "/Users/Shengfei/Desktop/cerebralcortex/data/"])
    # master_num += 1
    # port_num += 2

    while(True):
        # print ("I M THE BEST")
        end = time.time()
        if(end - start >= interval):
            file_list = os.listdir(datapath)
			# print file_list
            for file_name in file_list:
                print ("I got file")

                shutil.move(os.path.join(datapath, file_name), os.path.join(archive_path, file_name))

                os.environ['SPARK_IDENT_STRING'] = 'master{}'.format(master_num)
                # subprocess.call(["sudo", "sh", "/Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh"])
                # subprocess.call(["sudo", "sh", "/Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh"])
                # subprocess.call(["sudo", "sh", "/Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh", "-h", "127.0.0.1", "-p", "{}".format(port_num)])
                # subprocess.call(["sudo", "sh", "/Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh", "spark://127.0.0.1:{}".format(port_num)])

                subprocess.Popen(["spark-submit", "--conf", "spark.cores.max=1", \
				"--master", "spark://127.0.0.1:{}".format(port_list[port_idx]), \
				"--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1", \
				"ss_session.py",
				"/Users/Shengfei/Desktop/cerebralcortex/data/", \
				"{}".format(archive_path), "{}".format(file_name), \
				"{}".format(cid) \
                ])
                port_idx += 1
                master_num += 1
                port_num += 2
                cid += 1
            start = time.time()

if __name__ == "__main__":
    check_virtual(5, "../VirtualSensor/", "../Archive/") # interval, path, archive_path

#
# import shutil
# import time
# import os
# import subprocess
#
# def check_virtual(interval: int, datapath: str, archive_path: str):
#     start = time.time()
#     master_num = 1
#     cid = 1
#     port_num = 8081
#     #list=[8082,8084,8084,8086]
#     #portcount = 0
#     while(True):
#         # print ("I M THE BEST")
#         end = time.time()
#         if(end - start >= interval):
#             file_list = os.listdir(datapath)
# 			# print file_list
#             for file_name in file_list:
#                 print ("I got file")
#
#                 shutil.move(os.path.join(datapath, file_name), os.path.join(archive_path, file_name))
#                 os.environ['SPARK_IDENT_STRING'] = 'master{}'.format(master_num)
#                 # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh"])
#                 # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh"])
#                 subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh", "-h", "127.0.0.1", "-p", "{}".format(port_num)])
#                 subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh", "spark://127.0.0.1:{}".format(port_num)])
#                 #list[count]
#                 #count++
#                 subprocess.Popen(["spark-submit", "--conf", "spark.cores.max=1", \
# 				"--master", "spark://127.0.0.1:{}".format(port_num), \
# 				"--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1", \
# 				"ss_session.py",
# 				"/Users/Shengfei/Desktop/cerebralcortex/data/", \
# 				"{}".format(archive_path), "{}".format(file_name), \
# 				"{}".format(cid) \
#                 ])
#                 master_num += 1
#                 port_num += 2
#                 cid += 1
#             start = time.time()
#
# if __name__ == "__main__":
#     check_virtual(5, "../VirtualSensor/", "../Archive/") # interval, path, archive_path
