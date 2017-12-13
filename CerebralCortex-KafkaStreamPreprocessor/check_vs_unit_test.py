# import subprocess
# subprocess.call(["spark-submit", "--conf", "spark.cores.max=1", \
# 	"--master", "spark://127.0.0.1:8081", \
# 	"--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1", \
# 	"main1.py",
# 	"/Users/jingxianxu/Desktop/cerebralcortex/data/" \
# 	])

import shutil
import time
import os
import subprocess

def check_virtual(interval: int, datapath: str, archive_path: str):
    start = time.time()
    master_num = 1
    cid = 1
    port_num = 8081

    # os.environ['SPARK_IDENT_STRING'] = 'master{}'.format(master_num)
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh"])
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh"])
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh", "-h", "127.0.0.1", "-p", "{}".format(port_num)])
    # subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh", "spark://127.0.0.1:{}".format(port_num)])
    #
    # subprocess.call(["spark-submit", "--conf", "spark.cores.max=1", \
    # "--master", "spark://127.0.0.1:{}".format(port_num), \
    # "--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1", \
    # "test.py",
    # "/Users/Shengfei/Desktop/cerebralcortex/data/"
    # ])
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
                subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh"])
                subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh"])
                subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh", "-h", "127.0.0.1", "-p", "{}".format(port_num)])
                subprocess.call(["sudo", "sh", "/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh", "spark://127.0.0.1:{}".format(port_num)])

                subprocess.call(["spark-submit", "--conf", "spark.cores.max=1", \
				"--master", "spark://127.0.0.1:{}".format(port_num), \
				"--packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1", \
				"create_session.py",
				"/Users/Shengfei/Desktop/cerebralcortex/data/", \
				"{}".format(archive_path), "{}".format(file_name), \
				"{}".format(cid) \
                ])
                master_num += 1
                port_num += 2
                cid += 1
            start = time.time()

if __name__ == "__main__":
    check_virtual(10, "../VirtualSensor/", "../Archive/") # interval, path, archive_path
