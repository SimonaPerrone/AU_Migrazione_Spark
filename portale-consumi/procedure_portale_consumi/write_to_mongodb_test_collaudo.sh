#!/bin/bash

LOG_F="test_write_mongodb.log"

 cd /mnt/isilonshare1/Software/EE/bin

 #./flusso-misure-pc_mongo_test.sh -PC_EX FG-C &> "$LOG_F"
 #./flusso-misure-pc_mongo_test.sh -PC_EX F-C &>> "$LOG_F"
 #./flusso-misure-pc_mongo_test.sh -PC_EX MG-SPLIT33-C &>> "$LOG_F"
 #./flusso-misure-pc_mongo_test.sh -PC_EX MG-SPLIT3-C &>> "$LOG_F"
 #./flusso-misure-pc_mongo_test.sh -PC_EX M-SPLIT3-C &> "$LOG_F"
 #./flusso-misure-pc_mongo_test.sh -PC_EX MG-SPLIT33-C &>> "$LOG_F"
 ./flusso-misure-pc_mongo_test.sh -PC_EX M-SPLIT33-C &> "$LOG_F"
 #./flusso-misure-pc_mongo_test.sh -PC_EX I-C -l
