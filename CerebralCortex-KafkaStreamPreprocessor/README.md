# CerebralCortex-KafkaStreamPreprocessor
CerebralCortex-KafkaStreamPreprocessor (CC-KSP) is a apache-spark based pub/sub system for processing incoming mobile sensor data.

## How it works?
* CerebralCortex-APIServer uploads and publish publish file names on Kafka queue.
* CC-KSP process consumes messages published by [CerebralCortex-APIServer](https://github.com/MD2Korg/CerebralCortex-APIServer)  and:
    * Processes .gz files uploaded using Processed data
    * Converts CSV/JSON data into CerebralCortex stream format
    * Stores processed raw data and metadata in Cassandra and MySQL using CerebralCortex [DataStorageEngine](https://github.com/MD2Korg/CerebralCortex/tree/master/cerebralcortex/kernel/DataStoreEngine)
    * Stores processed data in InfluxDB for visualization
    
## How to run?
### Setup environment
To run CC-KSP, Clone/install and configure:
* [Python3](https://www.python.org/download/releases/3.0/)
* [CerebralCortex-DockerCompose](https://github.com/MD2Korg/CerebralCortex-DockerCompose)
* [Apache Spark 2.1.0](https://spark.apache.org/releases/spark-release-2-1-0.html) 
* [CerebralCortex](https://github.com/MD2Korg/CerebralCortex)

### Clone and configure CC-KSP
* git clone https://github.com/MD2Korg/CerebralCortex-KafkaStreamPreprocessor.git
* Configure database url, username, and passwords in [cerebralcortex_apiserver.yml](https://github.com/MD2Korg/CerebralCortex-KafkaStreamPreprocessor/blob/master/cerebralcortex_apiserver.yml)
    * Please do not change other options in configurations unless you have changed them in CerebralCortex-DockerCompose   
* Install dependencies:
    * pip install -r requirements.txt
* Run following command to start CC-KSP:
    * sh run.sh (Please update the paths in run.sh, read comments)


