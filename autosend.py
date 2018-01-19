import shutil
import datetime
import time
import gzip
import os
import subprocess
import queue
import threading



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
# def auto_send(interval: int, datapoolpath: str, destination_path: str):

#     start = time.time()
#     end = start + interval #seconds
#     file_id = 0
#     print ("Now: "+str(datetime.datetime.fromtimestamp(time.time())))

#     while(time.time() <= end):
#         #file_name = "test"+str(file_id)+".gz"
#         #file_meta = "test"+str(file_id)+".json"
#         file_name = "test"+"000"+".gz"
#         file_meta = "test"+"000"+".json"

#         if file_name in os.listdir(datapoolpath):
#             gzip_file_content = get_gzip_file_contents(datapoolpath + file_name)
#             datapoints = list(map(lambda x: row_to_datapoint_cus(x), gzip_file_content.splitlines()))
#             start_time = datapoints[0]["time"]
#             end_time = datapoints[len(datapoints) - 1]["time"]
#             duration = end_time - start_time +1
#             print ("Sending in", duration, "seconds")
#             time.sleep(duration)

#             shutil.copy(os.path.join(datapoolpath, file_name), os.path.join(destination_path, file_name))
#             shutil.copy(os.path.join(datapoolpath, file_meta), os.path.join(destination_path, file_meta))
#             print (str(datetime.datetime.fromtimestamp(time.time())))
#             subprocess.call(["python3", "/Users/Shengfei/Desktop/cerebralcortex/test.py"])
#             os.remove(os.path.join(destination_path, file_name))
#             os.remove(os.path.join(destination_path, file_meta))
#             time.sleep(1)
#             file_id += 1
def auto_send(datapoolpath, destination_path, file_queue):
    start_time = -1
    elapsed = 0

    while not file_queue.empty():
        end_time, start, file_name = file_queue.get()
        file_meta = file_name.replace(".gz", ".json")
        print(end_time, file_meta)

        if start_time == -1:
            start_time = start

        duration = end_time - start_time + 1 - elapsed
        elapsed += duration
        print ("Sending in", duration, "seconds")
        time.sleep(duration)

        shutil.copy(os.path.join(datapoolpath, file_name), os.path.join(destination_path, file_name))
        shutil.copy(os.path.join(datapoolpath, file_meta), os.path.join(destination_path, file_meta))

        subprocess.call(["python3", "/Users/Shengfei/Desktop/cerebralcortex/test.py"])
        os.remove(os.path.join(destination_path, file_name))
        os.remove(os.path.join(destination_path, file_meta))

def read_data(datapoolpath: str, destination_path: str):
    queue_array = [queue.PriorityQueue() for x in range(3)]

    for file_name in os.listdir(datapoolpath):
        if ".gz" in file_name:
            sid = int(file_name[4])
            #fid = int(file_name[5:7])
            gzip_file_content = get_gzip_file_contents(datapoolpath + file_name)
            datapoints = list(map(lambda x: row_to_datapoint_cus(x), gzip_file_content.splitlines()))
            start_time = datapoints[0]["time"]
            end_time = datapoints[-1]["time"]
            queue_array[sid].put((end_time, start_time, file_name))

    # Create 3 threads
    threads = []
    try:
        for i in range(3):
            t = threading.Thread(target=auto_send, args=[datapoolpath, destination_path, queue_array[i]])
            t.start()
            threads.append(t)
    except:
       print("Error: unable to start thread")

    for t in threads:
        t.join()
    
if __name__ == "__main__":
    read_data("./Datapool/", "./SampleData/")
