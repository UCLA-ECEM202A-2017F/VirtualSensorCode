# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import time
import sys
from pyspark import SparkContext
#from core import CC
#from core.kafka_consumer import spark_kafka_consumer
#from core.kafka_to_cc_storage_engine import kafka_to_db
from pyspark.streaming import StreamingContext
#from core.kafka_producer import kafka_file_to_json_producer


##########################################
import json
import os
from cerebralcortex.kernel.datatypes.datastream import DataStream
from datetime import datetime
from cerebralcortex.kernel.utils.logging import cc_log

###################################
from cerebralcortex.CerebralCortex import CerebralCortex

#Sandeep: Give path to .yml file of APIServer
configuration_file = os.path.join(os.path.dirname(__file__), 'cerebralcortex_apiserver.yml')
CC = CerebralCortex(configuration_file, time_zone="America/Los_Angeles", load_spark=False)

###################################

import json

#from core import CC
from pyspark.streaming.kafka import KafkaDStream
#from core.kafka_offset import storeOffsetRanges
from cerebralcortex.kernel.utils.logging import cc_log
import datetime

def verify_fields(msg):
    if "metadata" in msg and "data" in msg:
#        print("Batch size " + str(len(msg["data"])))
        return True
    return False


def store_streams(data):
    try:
        st = datetime.datetime.now()
        CC.save_datastream_to_influxdb(data)
        CC.save_datastream(data, "json")
        print("Stream Saved: ", data['filename'], (datetime.datetime.now()-st))
    except:
        cc_log()

# def kafka_to_db(message: KafkaDStream):
#     """
#
#     :param message:
#     """
#     records = message.map(lambda r: json.loads(r[1]))
#     valid_records = records.filter(verify_fields)
#
#     valid_records.foreach(lambda stream_data: store_streams(stream_data))
#
#     storeOffsetRanges(message)
#
#     print("Ready to process stream...")

##################################

#util module files
import datetime
import gzip
import json
from pympler import asizeof

from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint
from dateutil.parser import parse


def get_gzip_file_contents(file_name: str) -> str:
    """
    Read and return gzip compressed file contents
    :param file_name:
    :return:
    """
    fp = gzip.open(file_name)
    gzip_file_content = fp.read()
    fp.close()
    gzip_file_content = gzip_file_content.decode('utf-8')
    return gzip_file_content


def chunks(data: str, max_len: int) -> str:
    """
    Yields max_len sized chunks with the remainder in the last
    :param data:
    :param max_len:
    """
    for i in range(0, len(data), max_len):
        yield data[i:i + max_len]

def get_chunk_size(data):

    if len(data) > 0:
        chunk_size = 750000/(asizeof.asizeof(data)/len(data)) #0.75MB chunk size without metadata
        return round(chunk_size)
    else:
        return 100

def row_to_datapoint(row: str) -> dict:
    """
        Format data based on mCerebrum's current GZ-CSV format into what Cerebral
    Cortex expects
    :param row:
    :return:
    """
    ts, offset, values = row.split(',', 2)
    ts = int(ts) / 1000.0
    offset = int(offset)

    if isinstance(values, tuple):
        values = list(values)
    else:
        try:
            values = json.loads(values)
        except:
            try:
                values = [float(values)]
            except:
                try:
                    values = list(map(float, values.split(',')))
                except:
                    values = values

    timezone = datetime.timezone(datetime.timedelta(milliseconds=offset))
    ts = datetime.datetime.fromtimestamp(ts, timezone)
    return DataPoint(start_time=ts, sample=values)
    #return {'starttime': str(ts), 'value': values}


def rename_file(old: str):
    """

    :param old:
    """
    old_file_name = old.rsplit('/', 1)[1]
    new_file_name = "PROCESSED_" + old_file_name
    new_file_name = str.replace(old, old_file_name, new_file_name)
    # if os.path.isfile(old):
    #     os.rename(old, new_file_name)


##########################
def json_to_datapoints(json_obj):
    if isinstance(json_obj["value"], str):
        sample = json_obj["value"]
    else:
        sample = json.dumps(json_obj["value"])
    start_time = parse(json_obj["starttime"])

    if "endtime" in json_obj:  # Test-code, this if will not be executed
        return DataPoint(start_time=start_time, end_time=json_obj["endtime"], sample=sample)
    else:
        return DataPoint(start_time=start_time, sample=sample)


def json_to_datastream(json_obj, stream_type):
    data = json_obj["data"]
    metadata = json_obj["metadata"]
    identifier = metadata["identifier"]
    owner = metadata["owner"]
    name = metadata["name"]
    data_descriptor = metadata["data_descriptor"]
    execution_context = metadata["execution_context"]
    annotations = metadata["annotations"]
    stream_type = stream_type
    start_time = data[0]["starttime"]
    end_time = data[len(data) - 1]["starttime"]
    datapoints = list(map(json_to_datapoints, data))

    return DataStream(identifier,
                      owner,
                      name,
                      data_descriptor,
                      execution_context,
                      annotations,
                      stream_type,
                      start_time,
                      end_time,
                      datapoints)

#################################
import json
import os
from cerebralcortex.kernel.datatypes.datastream import DataStream
from datetime import datetime
from cerebralcortex.kernel.utils.logging import cc_log

#from core import CC
#from core.kafka_offset import storeOffsetRanges
from pyspark.streaming.kafka import KafkaDStream
#from util.util import row_to_datapoint, chunks, get_gzip_file_contents, rename_file


def verify_fields(msg: dict, data_path: str) -> bool:
    """
    Verify whether msg contains file name and metadata
    :param msg:
    :param data_path:
    :return:
    """
    if "metadata" in msg and "filename" in msg:
        if os.path.isfile(data_path + msg["filename"]):
            return True
    return False


def file_processor(msg: dict, data_path: str) -> DataStream:
    """
    :param msg:
    :param data_path:
    :return:
    """
    if not isinstance(msg["metadata"],dict):
        metadata_header = json.loads(msg["metadata"])
    else:
        metadata_header = msg["metadata"]

    identifier = metadata_header["identifier"]
    owner = metadata_header["owner"]
    name = metadata_header["name"]
    data_descriptor = metadata_header["data_descriptor"]
    execution_context = metadata_header["execution_context"]
    if "annotations" in metadata_header:
        annotations = metadata_header["annotations"]
    else:
        annotations={}
    if "stream_type" in metadata_header:
        stream_type = metadata_header["stream_type"]
    else:
        stream_type = "ds"

    try:
        gzip_file_content = get_gzip_file_contents(data_path + msg["filename"])
        datapoints = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))
        rename_file(data_path + msg["filename"])

        start_time = datapoints[0].start_time
        end_time = datapoints[len(datapoints) - 1].end_time

        return DataStream(identifier,
                          owner,
                          name,
                          data_descriptor,
                          execution_context,
                          annotations,
                          stream_type,
                          start_time,
                          end_time,
                          datapoints)
    except Exception as e:
        error_log = "In Kafka preprocessor - Error in processing file: " + str(msg["filename"])+" Owner-ID: "+owner + "Stream Name: "+name + " - " + str(e)
        cc_log(error_log, "MISSING_DATA")
        datapoints = []
        return None


def store_stream(data: DataStream):
    """
    Store data into Cassandra, MySQL, and influxDB
    :param data:
    """
    if data:
        try:
            c1 = datetime.now()
            CC.save_datastream(data,"datastream")
            e1 = datetime.now()
            CC.save_datastream_to_influxdb(data)
            i1 = datetime.now()
            print("Cassandra Time: ", e1-c1, " Influx Time: ",i1-e1, " Batch size: ",len(data.data))
        except:
            cc_log()


def kafka_file_to_json_producer(message: KafkaDStream, data_path):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """
    records = message.map(lambda r: json.loads(r[1]))
    valid_records = records.filter(lambda rdd: verify_fields(rdd, data_path))
    results = valid_records.map(lambda rdd: file_processor(rdd, data_path)).map(
        store_stream)

    storeOffsetRanges(message)

    print("File Iteration count:", results.count())

#################################

#from core import CC
from cerebralcortex.kernel.utils.logging import cc_log

def storeOffsetRanges(rdd):
    offsetRanges = rdd.offsetRanges()
    for offsets in offsetRanges:
        try:
            CC.store_or_update_Kafka_offset(offsets.topic, offsets.partition, offsets.fromOffset, offsets.untilOffset)
        except:
            cc_log()

################################
from pyspark.streaming.kafka import KafkaUtils, KafkaDStream, OffsetRange, TopicAndPartition
#from core import CC

def spark_kafka_consumer(kafka_topic: str, ssc, broker, consumer_group_id) -> KafkaDStream:
    """
    supports only one topic at a time
    :param kafka_topic:
    :return:
    """
    try:
        offsets = CC.get_kafka_offsets(kafka_topic[0])

        if bool(offsets):
            fromOffset = {}
            for offset in offsets:
                offset_start = offset["offset_start"]
                offset_until = offset["offset_until"]
                topic_partition = offset["topic_partition"]
                topic = offset["topic"]

                topicPartion = TopicAndPartition(topic,int(topic_partition))
                fromOffset[topicPartion] = int(offset_start)

            return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                 {"metadata.broker.list": broker,
                                                  "group.id": consumer_group_id},fromOffsets=fromOffset)
        else:
            offset_reset = "smallest"  # smallest OR largest
            return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                 {"metadata.broker.list": broker, "auto.offset.reset":offset_reset,
                                                  "group.id": consumer_group_id})
    except Exception as e:
        print(e)

##################################
# from core import CC
# from core.kafka_consumer import spark_kafka_consumer
# from core.kafka_to_cc_storage_engine import kafka_to_db
# from pyspark.streaming import StreamingContext
# from core.kafka_producer_cus import process_valid_file
##################################

## User Defined Query Start ##
# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance():
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.appName("Cerebral-Cortex").getOrCreate()
        # SparkSession \
        #     .builder \
        #     .config(conf=sparkConf) \
        #     .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def verify_sid(msg: dict, sid: str, data_path: str) -> bool:
    if not isinstance(msg["metadata"], dict):
        metadata_header = json.loads(msg["metadata"])
    else:
        metadata_header = msg["metadata"]

    identifier = metadata_header["identifier"] # unique identifier
    if identifier == sid:
        return True
    return False

def extract_info(msg: dict, data_path: str) -> list:
        owner = metadata_header["owner"]
        name = metadata_header["name"]
        data_descriptor = metadata_header["data_descriptor"]
        execution_context = metadata_header["execution_context"]
        try:
            gzip_file_content = get_gzip_file_contents(data_path + msg["filename"])
            datapoints = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))
            start_time = datapoints[0].start_time
            end_time = datapoints[len(datapoints) - 1].end_time
            return [identifier, owner, name, data_descriptor, start_time, end_time, datapoints]

        except Exception as e:
            error_log = "In Kafka preprocessor - Error in processing file: " + str(msg["filename"])+" Owner-ID: "+owner + "Stream Name: "+name + " - " + str(e)
            cc_log(error_log, "MISSING_DATA")
            datapoints = []
            return None

#######################
def process(rdd: list):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance()

        ##### Example process
        myRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
        #### End of example

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
        wordCountsDataFrame.show()
    except:
        pass

# # The virtual sensor that only process on one stream
# def user_define_virtual_type_one(sid: str, msg: dict, datapath):
#     stream = verify_sid(msg, sid, datapath)
#     for data in stream:
#         # process here for average/min/max (define process)
#         result = # function #
#         # save result to database
#     return result
#
# def take_user_input
#
# def virtual_sensor(sid_in(one or multiple), sid_out, process(time, function))
# # end of User Defined Query ##

# def import_from(path):
#     """
#     Import an attribute, function or class from a module.
#     :attr path: A path descriptor in the form of 'pkg.module.submodule:attribute'
#     :type path: str
#     """
#     path_parts = path.split(':')
#     if len(path_parts) < 2:
#         raise ImportError("path must be in the form of pkg.module.submodule:attribute")
#     module = __import__(path_parts[0], fromlist=path_parts[1])
#     return getattr(module, path_parts[1])

def process_valid_file(message: KafkaDStream, data_path: str, sensor_id: str):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """
    records = message.map(lambda r: json.loads(r[1])) # filename
    valid_records = records.filter(lambda rdd: verify_fields(rdd, data_path))
    valid_sensors = valid_records.filter(lambda rdd: verify_sid(rdd, sid, data_path))
    results = valid_sensors.map(lambda rdd: file_processor(rdd, data_path))) # rdd of list [identifier, owner, name, data_descriptor, start_time, end_time, datapoints]
    print ("Great")
    results.map(lambda rdd: process(rdd))


def read_udf(data_path: str, file_name: str):
    with open(data_path+file_name) as json_data:
        dt = json.load(json_data)
        sid = dt['input_id']
        osid = dt['output_id']
        time_interval = dt['interval']
        endt = dt['endtime']
        process = dt['process']
    return [sid, osid, time_interval, endt, process]

# =============================================================================
# Kafka Consumer Configs
batch_duration = 5  # seconds
sc = SparkContext("spark://127.0.0.1:8083", "Cerebral-Cortex")
# master_port = sys.argv[5]
# sc = SparkContext(master_port, "Cerebral-Cortex")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, batch_duration)
# spark = SparkSession.builder.appName("kakade").getOrCreate()
broker = "localhost:9092"  # multiple brokers can be passed as comma separated values

data_path = sys.argv[1]
if (data_path[-1] != '/'):
    data_path += '/'

archive_path = sys.argv[3]
if (archive_path[-1] != '/'):
    archive_path += '/'

group_id = sys.argv[4]
consumer_group_id = "md2k-test" + str(group_id)

file_name = sys.argv[2]

virtual_sensor = read_udf(archive_path, file_name)
sensor_id = virtual_sensor["sid"]

kafka_files_stream = spark_kafka_consumer(["filequeue"], ssc, broker, consumer_group_id)
kafka_files_stream.foreachRDD(lambda rdd: process_valid_file(rdd, data_path, sensor_id)) # store or create DF() process -> type 0

# name = kafka_files_stream.map(lambda rdd: ) # get file name and then use structured streaming type 1
# record = spark.readStream.csv("/Users/Shengfei/Desktop/cerebralcortex/data/"+name) # todo: add schema

# ssc.start()
# ssc.awaitTermination()
