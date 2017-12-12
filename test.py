import requests
import json
from os import listdir
from os.path import isfile, join

API_ENDPOINT_AUTH="http://0.0.0.0:8088/api/v1/auth/"

credentials={"username": "string", "password":"473287f8298dba7163a897908958f7c0eae733e25d2e027992ea2edc9bed2fa8"}
response = requests.post(url=API_ENDPOINT_AUTH,json=credentials)

auth_token = ""
json_response_dict = response.json()
auth_token = json_response_dict["access_token"]

print("The auth token is: %s"%auth_token)

API_ENDPOINT_DATA="http://0.0.0.0:8088/api/v1/stream/zip/"
headers = {'Authorization': auth_token}
data_dir = "SampleData/"

onlyfiles = [f for f in listdir(data_dir) if (join(data_dir, f)).endswith('.json')]
for payload_file in onlyfiles:
	uploaded_file = dict(file=open(data_dir + payload_file.replace('.json','.gz'), 'rb'))
	#print(uploaded_file)
	#print(payload_file)
	metadata = open(data_dir + payload_file, 'r')
	response = requests.put(url=API_ENDPOINT_DATA,  files=uploaded_file,data={'metadata': json.dumps(metadata.read())}, headers=headers)
	print(response.text)
	uploaded_file['file'].close()
	metadata.close()
