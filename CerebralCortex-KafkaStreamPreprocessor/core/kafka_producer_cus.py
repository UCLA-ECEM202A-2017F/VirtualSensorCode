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

import json
import os
from cerebralcortex.kernel.datatypes.datastream import DataStream
from datetime import datetime
from cerebralcortex.kernel.utils.logging import cc_log

from core import CC
from core.kafka_offset import storeOffsetRanges
from pyspark.streaming.kafka import KafkaDStream
from util.util import row_to_datapoint, chunks, get_gzip_file_contents, rename_file


# def verify_fields(msg: dict, data_path: str) -> bool:
#     """
#     Verify whether msg contains file name and metadata
#     :param msg:
#     :param data_path:
#     :return:
#     """
#     if "metadata" in msg and "filename" in msg:
#         if os.path.isfile(data_path + msg["filename"]):
#             return True
#     return False
#
#
# def file_processor(msg: dict, data_path: str) -> DataStream:
#     """
#     :param msg:
#     :param data_path:
#     :return:
#     """
#     if not isinstance(msg["metadata"],dict):
#         metadata_header = json.loads(msg["metadata"])
#     else:
#         metadata_header = msg["metadata"]
#
#     identifier = metadata_header["identifier"]
#     owner = metadata_header["owner"]
#     name = metadata_header["name"]
#     data_descriptor = metadata_header["data_descriptor"]
#     execution_context = metadata_header["execution_context"]
#     if "annotations" in metadata_header:
#         annotations = metadata_header["annotations"]
#     else:
#         annotations={}
#     if "stream_type" in metadata_header:
#         stream_type = metadata_header["stream_type"]
#     else:
#         stream_type = "ds"
#
#     try:
#         gzip_file_content = get_gzip_file_contents(data_path + msg["filename"])
#         datapoints = list(map(lambda x: row_to_datapoint(x), gzip_file_content.splitlines()))
#         rename_file(data_path + msg["filename"])
#
#         start_time = datapoints[0].start_time
#         end_time = datapoints[len(datapoints) - 1].end_time
#
#         return DataStream(identifier,
#                           owner,
#                           name,
#                           data_descriptor,
#                           execution_context,
#                           annotations,
#                           stream_type,
#                           start_time,
#                           end_time,
#                           datapoints)
#     except Exception as e:
#         error_log = "In Kafka preprocessor - Error in processing file: " + str(msg["filename"])+" Owner-ID: "+owner + "Stream Name: "+name + " - " + str(e)
#         cc_log(error_log, "MISSING_DATA")
#         datapoints = []
#         return None
#
#
# def store_stream(data: DataStream):
#     """
#     Store data into Cassandra, MySQL, and influxDB
#     :param data:
#     """
#     if data:
#         try:
#             c1 = datetime.now()
#             CC.save_datastream(data,"datastream")
#             e1 = datetime.now()
#             CC.save_datastream_to_influxdb(data)
#             i1 = datetime.now()
#             print("Cassandra Time: ", e1-c1, " Influx Time: ",i1-e1, " Batch size: ",len(data.data))
#         except:
#             cc_log()

# ## User Defined Query ##
def verify_sid(msg: dict, sid: str, data_path): -> list
    if not isinstance(msg["metadata"],dict):
        metadata_header = json.loads(msg["metadata"])
    else:
        metadata_header = msg["metadata"]

    identifier = metadata_header["identifier"] # unique identifier

    if identifier == sid:
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

# The virtual sensor that process accross time
def user_define_virtual_type_two(sid: str, mapp: dict, data_path):
    conf = SparkConf().setAppName("Reading files from a directory")
    sc   = SparkContext(conf=conf)
    ssc  = StreamingContext(sc, 2)
    filename = mapp[sid] # only one file
    lines = ssc.textFileStream(data_path)

    # Add header to data feature
    schema = ["identifier", "owner", "name", "data_descriptor", "start_time", "end_time", "datapoints"]
    df = lines.map(lambda rdd: verify_sid(rdd, sid, data_path)).toDF(schema) # check if the file has the desired sensor id (sid) transfer to DF and process
    df = df.select("start_time", "end_time", "datapoints")
    # Process the avg here #
    # Process the max here #
    # Process the min here #
    # define process for across time virtual sensor
    # store into the database

# The virtual sensor that only process on one stream
def user_define_virtual_type_one(sid: str, msg: dict, datapath):
    stream = verify_sid(msg, sid, datapath)
    for data in stream:
        # process here for average/min/max (define process)
        result = # function #
        # save result to database
    return result

def take_user_input

def virtual_sensor(sid_in(one or multiple), sid_out, process(time, function))
# end of User Defined Query ##

def import_from(path):
    """
    Import an attribute, function or class from a module.
    :attr path: A path descriptor in the form of 'pkg.module.submodule:attribute'
    :type path: str
    """
    path_parts = path.split(':')
    if len(path_parts) < 2:
        raise ImportError("path must be in the form of pkg.module.submodule:attribute")
    module = __import__(path_parts[0], fromlist=path_parts[1])
    return getattr(module, path_parts[1])
# func = import_from('a.b.c.d.myfile:mymethod')
#     func()

# def read_udf (data_path) -> list:
#     with open(data_path + 'input.json') as json_data:
#         d = json.load(json_data)
#         sid = d['From_id']
#         osid = d['Output_id']
#         time_interval = d['Every']
#         last = d['Last']
#         process = d['Process_fuunc_file_name']
#     return [sid, osid, time_interval, last, process]
# end of User Difined Query

def process_valid_file(message: KafkaDStream, data_path: str, sensor_id: str):
    """
    Read convert gzip file data into json object and publish it on Kafka
    :param message:
    """
    records = message.map(lambda r: json.loads(r[1]))
    valid_records = records.filter(lambda rdd: verify_fields(rdd, data_path))
    results = valid_records.map(lambda rdd: file_processor(rdd, data_path)).map(store_stream)

    storeOffsetRanges(message)

    print("File Iteration count:", results.count())
