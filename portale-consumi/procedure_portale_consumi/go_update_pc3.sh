#!/bin/bash

CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_3_$CURTMS"".txt"
LOG_FILE_GAS="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_3_$CURTMS""_GAS.txt"

limit_date=$(date -d "-125 days" +%Y-%m-%d)

numero_settimana=`date +\%-W`

if [[ ($((`date +\%-W` % 4)) -ne 0) ]]
then
	echo "Elaborazione pc3" >> "$LOG_FILE"
	/mnt/isilonshare1/Software/EE/portale_consumi/run_update_pc.sh "$LOG_FILE" "$LOG_FILE_GAS" "$limit_date" "$limit_date" "1"
else
	echo "settimana non adatta per Elaborazione pc3" >> "$LOG_FILE"
fi
