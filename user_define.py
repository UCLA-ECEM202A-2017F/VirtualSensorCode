
import re
import os.path
import json

output_folder_path = './'
json_output_file = 'user_query3.json'

def prompt(message, isvalid):
    """Prompt for input given a message and return that value after verifying the input.

    Keyword arguments:
    message -- the message to display when asking the user for the value
    isvalid -- a function that returns True if the value given by the user is valid
    """
    res = None
    while res is None:
        res = input(str(message)+': ')
        if not isvalid(res):
            print("invalid input")
            res = None
    return res


def process_input():
    input_id = prompt(
            message = "Enter the input sensor ID", 
            isvalid = lambda v : re.match(r"[0-9a-zA-Z]+", v)
            )

    output_id = prompt(
            message = "Enter the ouput stream ID", 
            isvalid = lambda v : re.match(r"[0-9a-zA-Z]+", v)
            )

    interval = prompt(
            message = "Enter the interval in seconds", 
            isvalid = lambda v : re.match(r"[0-9]+", v)
            )

    start_time = prompt(
            message = "Enter the start time in UNIX Timestamp", 
            isvalid = lambda v : re.match(r"[0-9]+", v)
            )

    end_time = prompt(
            message = "Enter the end time in UNIX Timestamp", 
            isvalid = lambda v : re.match(r"[0-9]+", v)
            )

    process = prompt(
            message = "Enter the process function", 
            isvalid = lambda v : re.match(r"[0-9a-zA-Z]+", v)
            )

    folder = prompt(
        message = "Enter the folder directory", 
        isvalid = lambda v : re.match(r"[0-9a-zA-Z]+", v)
        )

    window = prompt(
        message = "Enter the window", 
        isvalid = lambda v : re.match(r"[0-9]+", v)
        )

    sliding_window = prompt(
        message = "Enter the sliding window", 
        isvalid = lambda v : re.match(r"[0-9]+", v)
        )

    print(input_id, output_id, process)
    print(interval, start_time, end_time)

    data = {
            "input_id": input_id, 
            "output_id": output_id,
            "interval": interval,
            "start_time": start_time,
            "end_time": end_time,
            "process": process,
            "folder": folder,
            "window": window,
            "sliding_window": sliding_window
            }

    with open(output_folder_path+json_output_file, 'w') as outfile:
        json.dump(data, outfile)

if __name__ == "__main__":
    process_input()

# filename = prompt(
#         message = "Enter the path of the file to upload", 
#         errormessage= "The file path you provided does not exist",
#         isvalid = lambda v : os.path.isfile(v))

# dataset_name = prompt(
#         message = "Enter the name of the dataset you want to create", 
#         errormessage= "The dataset must be named",
#         isvalid = lambda v : len(v) > 0)
        # isvalid = lambda v : re.search(r"(([^-])+-){4}[^-]+", v))
