# VirtualSensorCode EEM202A

Please refer to: https://docs.google.com/document/d/1s3DBRgEEIY_mqR0ILydAJdFmiNjfmDeQzXpoN5Bn6C4/edit for initial setup

=========================================================

Step 1:

1) In setup.sh, modify the Spark path on line 7 and line 17~29
2) Add spark master-slave pair for each potential virtual sensor (note: master port must be odd number)
3) Once finish, run: sudo setup.sh Then the environment will be ready to use
4) Modify the path on line 619 in ss_session.py

Step 2a: How to run: (run a single virtual sensor)

1) In run.sh, modify the Spark path on line 7, set up the port for spark master (the one you start in step 1)
2) Specify the input (modify the data path) on line 17
3) About the input: 1) program to submit; 2) Data folder (same as in initial setup);
                    3) Virtual sensor check folder; 4) User query json file; 5) Kafka consumer group id
4) Once finish, run: sh run.sh

Step 2b: How to run: (run multiple virtual_sensors)

1) main_check is a process that periodically checking the arrival of new virtual sensor.
   In main_check.py, modify line 12 to add all the ports you have started in Step 1.
   Modify line 32 and line 33 for the file to submit and the data folder path. Modify line 44 for sensor path (Refer to Step 2a.3 for input spec)
   Once finish, run python3 main_check.py

2) Drag the user query json file to the first folder path in line 44 of main_check.py
   the system will catch the arrival of the virtual sensor within 5 seconds

3) Keep repeating the second step to add more virtual sensors, you should be able to see the output on the console

Step 3: (optional data input):

1) We provide sample data files in datapool folder, to send data based on the timespan for each file,
   use python3 autosend.py
2) Note that the datapool only contains 3 sensors' dataset. Ideally you use real data from the phone/Internet

=========================================================
Generating virtual sensor & user defined process:

1) Run python3 user_define.py to generate virtual sensor. 9 Input fields are supported (refer to the slide please)
2) Write your customized function in yourfunctionname.py and save the file in UDF folder.
3) Function format: expecting input for your function would contain two parts (gf and df). gf is the grouped data based on the window you
   specified in your virtual sensor, df is the spark dataframe for all historical data. Please apply whatever process you want on the DataFrame.
   The return type is also an aggregated spark dataframe.

=========================================================
Folder explaination:

1) VirtualSensor: the folder to drag your user query json file (virtual sensor) in
2) Archive: the user query json file will be transfered to here to be collected
3) data: Data folder (refer to initial setting)
4) Output: output file will go here (currently partially supported)
5) UDF: user defined function needs to be stored here
6) V0_data/V1_data...: These folder will be generated during runtime. It seperates the files required by each virtual sensor

=========================================================

On exit:

1) Please delete all the V0_data/V1_data.... files
2) Deleting the files in data folder is optinal
3) Stop all spark masters and slaves (refer to the script in main_chack.py)

=========================================================
Others:

**Datapool Data Format:

Arriving Files:
1. 23:35:16 ~ 23.35:25
2. 23:35:26 ~ 23.35:29
3. 23:35:30 ~ 23.35:36
4. 23:35:37 ~ 23.35:45
.
.
.
lasting for 5 minutes


Processing interval: As user defined 
ss_session.py utilizes spark structure streaming


**Questiones:
Welcome to send any questions to syu93@ucla.edu
