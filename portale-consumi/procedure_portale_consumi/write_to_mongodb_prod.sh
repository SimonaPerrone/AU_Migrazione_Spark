#!/bin/bash

LOG_F="write_prod_mongodb.log"
LOF_F_GAS="write_prod_mongodb_gas.log"

 cd /mnt/isilonshare1/Software/EE/bin

 ./flusso-misure-pc_mongo.sh -PC_EX MG-SPLIT3-I &>> "$LOG_F_GAS" &
 ./flusso-misure-pc_mongo.sh -PC_EX M-SPLIT3-I &>> "$LOG_F"
