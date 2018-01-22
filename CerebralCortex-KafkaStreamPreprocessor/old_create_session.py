import time
import sys
from pyspark import SparkContext, SparkConf
#from core import CC
#from core.kafka_consumer import spark_kafka_consumer
#from core.kafka_to_cc_storage_engine import kafka_to_db
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover, CountVectorizer

##########################################
import json
import os
from cerebralcortex.kernel.datatypes.datastream import DataStream
from datetime import datetime
from cerebralcortex.kernel.utils.logging import cc_log
from threading import Thread
from importlib import import_module

###################################
from cerebralcortex.CerebralCortex import CerebralCortex

#Sandeep: Give path to .yml file of APIServer
configuration_file = os.path.join(os.path.dirname(__file__), 'cerebralcortex_apiserver.yml')
CC = CerebralCortex(configuration_file, time_zone="America/Los_Angeles", load_spark=False)

################################## Global variables
filelist = []
cur_time = 1513236910 #hard coded, should use datetime.now() in the future
# let user define start time

###################################
from pyspark.streaming.kafka import KafkaDStream
#from core.kafka_offset import storeOffsetRanges
from cerebralcortex.kernel.utils.logging import cc_log

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
import gzip
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
from cerebralcortex.kernel.datatypes.datastream import DataStream
from datetime import datetime
from cerebralcortex.kernel.utils.logging import cc_log
#from core.kafka_offset import storeOffsetRanges
from pyspark.streaming.kafka import KafkaUtils, KafkaDStream, OffsetRange, TopicAndPartition
from pyspark.sql import Row, SparkSession
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
def storeOffsetRanges(rdd):
    offsetRanges = rdd.offsetRanges()
    for offsets in offsetRanges:
        try:
            CC.store_or_update_Kafka_offset(offsets.topic, offsets.partition, offsets.fromOffset, offsets.untilOffset)
        except:
            cc_log()

################################
def spark_kafka_consumer(kafka_topic: str, ssc, broker, consumer_group_id) -> KafkaDStream:
    """
    supports only one topic at a time
    :param kafka_topic:
    :return:
    """
    try:
        offsets = CC.get_kafka_offsets(kafka_topic[0])
        offsets = False  # when out of range, reset
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
            offset_reset = "largest"  # smallest OR largest
            return KafkaUtils.createDirectStream(ssc, kafka_topic,
                                                 {"metadata.broker.list": broker, "auto.offset.reset":offset_reset,
                                                  "group.id": consumer_group_id})
    except Exception as e:
        print(e)

##################################
##        Virtual Sensor        ##
##################################
##   User Defined Query Start   ##

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


def row_to_datapoint_cus(row: str):
    ts, offset, values = row.split(', ', 2)
    # ts = int(ts) / 1000.0
    ts = int(ts)
    offset = int(offset)

    # if isinstance(values, tuple):
    #     values = list(values)
    # else:
    #     try:
    #         values = json.loads(values)
    #     except:
    #         try:
    #             values = [float(values)]
    #         except:
    #             try:
    #                 values = list(map(float, values.split(',')))
    #             except:
    #                 values = values

    # timezone = datetime.timezone(datetime.timedelta(milliseconds=offset))
    ts = datetime.fromtimestamp(ts)
    # return DataPoint(start_time=ts, sample=values)
    return {'time':str(ts), 'value':list(eval(values))}


def extract_info(msg: dict, data_path: str):
    global cur_time
    global interval

    try:
        metadata_header = msg["metadata"]
        filename = msg["filename"]
        #owner = "fbf8d50c-7f1d-47aa-b958-9caeadc676bd"#metadata_header["owner"]
        #name = metadata_header["name"]
        #data_descriptor = metadata_header["data_descriptor"]
        #execution_context = metadata_header["execution_context"]
        gzip_file_content = get_gzip_file_contents(data_path + msg["filename"])

        datapoints = list(map(lambda x: row_to_datapoint_cus(x), gzip_file_content.splitlines()))
        #print(datapoints)
        start_time = datapoints[0]["time"]
        end_time = datapoints[len(datapoints) - 1]["time"]

        # in the window, add into queue
        end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.timestamp(end_time)

        if end_time >= cur_time:
            return filename
            # filelist.append(filename)

        return None
        # if len(filelist) != 0:
        #     return [len(filelist)]

        #return [identifier, owner, name, data_descriptor, start_time, end_time, datapoints] #list of dictionary
        #return [0, owner, "name", "data_descriptor", start_time, end_time, datapoints]

        # return valid file name instead
        # return datapoints


    except Exception as e:
        #error_log = "In Kafka preprocessor - Error in processing file: " + str(msg["filename"])+" Owner-ID: "+owner + "Stream Name: "+name + " - " + str(e)
        cc_log(error_log, "MISSING_DATA")
        datapoints = []
        print(e)
        return None


#######################
def process(data: list):
    # print("========= %s =========" % str(time))
    try:
        print("====== In process ======")
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance()
        data = data.collect()
        rdd = spark.sparkContext.parallelize(data[0])
        # test dynamic import
        method(rdd)

        # # ====== original ===== #
        # rowRDD = rdd.map(lambda w: Row(time=w["time"], value=w["value"]))
        # df = spark.createDataFrame(rowRDD)
        # df.show()
        #
        # # ====== deprecated ===== #
        # # test = data.collect()
        # # rdd = spark.sparkContext.parallelize(test[0])
        # # df = rdd.toDF()
        #
        # ##### Example process
        # df.select(mean(df["value"][0]), mean(df["value"][1]), mean(df["value"][2])).show()
        # df.select(max(df["value"][0]), max(df["value"][1]), max(df["value"][2])).show()

    except Exception as e:
        print(e)


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


def process_valid_file(message: KafkaDStream, data_path: str, sensor_id: str, interval: int):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """

    print("====== Processing in process_valid_file ======")
    records = message.map(lambda r: json.loads(r[1])) # matadata & filename
    # print(records.collect())
    valid_records = records.filter(lambda rdd: verify_fields(rdd, data_path))
    print("File Iteration count-valid_records:", valid_records.count())

    print("====== Processing in verify_sid ======")
    valid_sensors = valid_records.filter(lambda rdd: verify_sid(rdd, sensor_id, data_path))
    print("File Iteration count-valid_sensors:", valid_sensors.count())
    # print(valid_sensors.collect())

    print("====== Processing in extract_info ======")
    results = valid_sensors.map(lambda rdd: extract_info(rdd, data_path))
    # used to be rdd of list [identifier, owner, name, data_descriptor, start_time, end_time, datapoints]
    # now just the file within window
    print("Result is: ")
    print(results.collect())
    print("File Iteration results:", results.count())

    ################### update buffer
    global filelist
    for f in results.collect():
        if f is not None:
            filelist.append(data_path+f)
    print ("File Length:", len(filelist))
    ###################
    # process(results) # serialized


def read_udf(data_path: str, file_name: str):
    with open(data_path+file_name) as json_data:
        dt = json.load(json_data)
        sid = dt['input_id']
        osid = dt['output_id']
        time_interval = dt['interval'] # output every interval time
        endt = dt['end_time']
        process = dt['process'] # module.process
    return [sid, osid, time_interval, endt, process]


###### preparing RDD/df for process ######
def compute_window_check(interval: int, datapath: str, filename: str): # sensor fields number generalize
    global cur_time
    global filelist
    time.sleep(interval)

    with open(filename, 'w') as myfile:
        myfile.write("Virtual Sensor Result:\n") # Future work: add description

    while(True):
        print ("=== Processing upon user's request ===")

        for f in filelist:
            print ("file:", f)

        output = "=== Window starting from: "+str(datetime.fromtimestamp(cur_time))+" with file length: "+str(len(filelist))+" ==="

        with open(filename, 'a') as myfile:
            myfile.write(output+'\n')

        print (output)
        # print ("window starting from:", datetime.fromtimestamp(cur_time), "with file length:", len(filelist))

        # filelist in not null (has file with that window)
        if len(filelist) != 0:
            path = ','.join(filelist)
            filelist = []
            spark = getSparkSessionInstance()
            sc = spark.sparkContext
            # sc.textFile(path).map(lambda x: x.replace('(', '').replace(')','').split(', ')).toDF().show(5)
            df = sc.textFile(path).map(lambda x: [list(eval(a)) if isinstance(eval(a),tuple) else eval(a) for a in x.split(', ',2)]).toDF(["TimeStamp","Offset","Value"])
            df = df.filter(df.TimeStamp>=cur_time)
            df.show()

            num = df.count()
            stime = df.select(min(df["TimeStamp"])).head()[0]
            etime = df.select(max(df["TimeStamp"])).head()[0]

            with open(filename, 'a') as myfile:
                myfile.write(">> "+str(num)+" Records Collected\n")
                myfile.write(">> Start time is: "+str(datetime.fromtimestamp(stime))+'\n')
                myfile.write(">> End time is: "+str(datetime.fromtimestamp(etime))+'\n')

            dfrdd = df.rdd.map(list)
            method(dfrdd)

            ###### test #####
            # filename = "28d64dad-2328-461c-8267-0e64ea6810fc.gz" # hard coded, use a buffer of file list in the future
            # schema = StructType([StructField("Timestamp", LongType()), \
            #                     StructField("Offset", StringType()), \
            #                     StructField("X", StringType()), \
            #                     StructField("Y", StringType()), \
            #                     StructField("Z", StringType())])
            # df = spark.read.format("csv").schema(schema).option("header","False").load(datapath+filename)
            # df.show()

        else:
            with open(filename, 'a') as myfile:
                myfile.write("Sorry. No data available\n")
            print ("Sorry. No data available")

        # path = "2e2578ed-e064-407e-b64b-085033700ec5.gz,28d64dad-2328-461c-8267-0e64ea6810fc.gz"
        # df = spark.read.format("csv").option("header","False").load(path.split(','))
        # df = sc.textFile("2e2578ed-e064-407e-b64b-085033700ec5.gz").map(lambda x: x.replace('(', '').replace(')','').split(', ')).toDF()
        # print ("window starting from: ", datetime.fromtimestamp(cur_time))
        cur_time += interval
        time.sleep(interval)

# =============================================================================
# Kafka Consumer Configs
batch_duration = 5  # seconds
# spark = SparkSession.builder.appName("xxx").getOrCreate()
# sc = SparkContext("spark://127.0.0.1:8083", "Cerebral-Cortex")
# master_port = sys.argv[5]
# sc = SparkContext(master_port, "Cerebral-Cortex")
sc = SparkContext(appName="Cerebral-Cortex")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, batch_duration)
broker = "localhost:9092"  # multiple brokers can be passed as comma separated values

data_path = sys.argv[1]
if (data_path[-1] != '/'):
    data_path += '/'

archive_path = sys.argv[2]
if (archive_path[-1] != '/'):
    archive_path += '/'

group_id = sys.argv[4]
consumer_group_id = "md2k-test"+str(group_id)

file_name = sys.argv[3]
virtual_sensor = read_udf(archive_path, file_name)

sensor_id = virtual_sensor[0]
interval = int(virtual_sensor[2])
udf_function = virtual_sensor[4]
result_file = "../Output"+virtual_sensor[1]

# Load user defined process
module_cus = import_module(udf_function)
# reload(module_cus)
method = getattr(module_cus, "process")
# method is now the function that could be used directly as method(testrdd)

print ("User Query -> Compute Window:"+str(interval)+", From:", sensor_id)
print ("User Query -> Detailed Info:", virtual_sensor)

compute = Thread(target=compute_window_check, args=(interval, data_path, result_file))
compute.start()

kafka_files_stream = spark_kafka_consumer(["filequeue"], ssc, broker, consumer_group_id)
kafka_files_stream.foreachRDD(lambda rdd: process_valid_file(rdd, data_path, sensor_id, interval)) # store or create DF() process -> type 0

# window & process
# name = kafka_files_stream.map(lambda rdd: ) # get file name and then use structured streaming type 1
# spark = getSparkSessionInstance()
# record = spark.readStream.csv("/Users/Shengfei/Desktop/cerebralcortex/data/"+name) # todo: add schema

ssc.start()
ssc.awaitTermination()
compute.join()

# /*// data records how many
# // simple function*/
