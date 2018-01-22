# VirtualSensorCode
202A

How to run: (single virtual sensor)

1. sh run.sh

2. python3 autosend.py (start sending data)

How to run: (multiple virtual sensor)

0. generate user query (virtual sensor json file)

1. sh source.sh (start master and confiure ports)

2. python3 main_check.py

3. python3 autosend (start sending data)

**Format:

Arriving Files:
1. 23:35:16 ~ 23.35:25
2. 23:35:26 ~ 23.35:29
3. 23:35:30 ~ 23.35:36
4. 23:35:37 ~ 23.35:45
.
.
.

Processing interval: As user defined (> 10s)

ss_session.py utilizes spark structure streaming
