# [](#header-1)Week 9

## [](#header-2)Where to implement Virtual Sensor?

Goal: Decouple user and sensor data source

Proposal 1: Implement a proxy between user and Cerebral.
Implement our own API server then send the data to API Server and Cerebral.

Proposal 2: In parallel with kafka. Challenge: Race condition on file I/O

Proposal 3: Modify Kafka. Make sure file deletions happen after processing.

Proposal 4: Query in Cassandra

Final Solution: Put queried data into cassandra. Build an API server to pass user query into data folder.
