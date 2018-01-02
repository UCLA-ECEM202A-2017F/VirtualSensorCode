import shutil
import datetime
import time
import gzip
import os
import subprocess


def get_gzip_file_contents(file_name: str):
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


def row_to_datapoint_cus(row: str):

    ts, offset, values = row.split(', ', 2)
    ts = int(ts)
    offset = int(offset)

    return {'time':ts, 'value':list(eval(values))}


# def check_virtual(interval: int, datapath: str, archive_path: str):
def auto_send(interval: int, datapoolpath: str, destination_path: str):

    start = time.time()
    end = start + interval #seconds
    file_id = 0
    print ("Now: "+str(datetime.datetime.fromtimestamp(time.time())))

    while(time.time() <= end):
        file_name = "test"+str(file_id)+".gz"
        file_meta = "test"+str(file_id)+".json"

        if file_name in os.listdir(datapoolpath):
            gzip_file_content = get_gzip_file_contents(datapoolpath + file_name)
            datapoints = list(map(lambda x: row_to_datapoint_cus(x), gzip_file_content.splitlines()))
            start_time = datapoints[0]["time"]
            end_time = datapoints[len(datapoints) - 1]["time"]
            duration = end_time - start_time
            print ("Sending in", duration, "seconds")
            time.sleep(duration)

            shutil.copy(os.path.join(datapoolpath, file_name), os.path.join(destination_path, file_name))
            shutil.copy(os.path.join(datapoolpath, file_meta), os.path.join(destination_path, file_meta))
            print (str(datetime.datetime.fromtimestamp(time.time())))
            subprocess.call(["python3", "/Users/Shengfei/Desktop/cerebralcortex/test.py"])
            os.remove(os.path.join(destination_path, file_name))
            os.remove(os.path.join(destination_path, file_meta))
            time.sleep(1)
            file_id += 1

if __name__ == "__main__":
    auto_send(25, "./Datapool/", "./SampleData/") # interval, path, archive_path
