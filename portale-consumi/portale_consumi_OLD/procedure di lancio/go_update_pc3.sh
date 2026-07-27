#!/bin/bash

CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_3_$CURTMS"".txt"
LOG_FILE_GAS="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_3_$CURTMS""_GAS.txt"

limit_date=$(date -d "-125 days" +%Y-%m-%d)

/mnt/isilonshare1/Software/EE/portale_consumi/run_update_pc.sh "$LOG_FILE" "$LOG_FILE_GAS" "$limit_date" "$limit_date" "1"


