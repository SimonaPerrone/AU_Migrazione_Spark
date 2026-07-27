#!/bin/bash

CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_36_$CURTMS"".txt"
LOG_FILE_GAS="/mnt/isilonshare1/Software/EE/portale_consumi/logs/log_PC_36_$CURTMS""_GAS.txt"


numero_settimana=`date +\%-W`
if [[ ($((`date +\%-W` % 4)) -eq 0) ]]
then
	echo "Elaborazione pc36" >> "$LOG_FILE"
	/mnt/isilonshare1/Software/EE/portale_consumi/run_update_pc.sh "$LOG_FILE" "$LOG_FILE_GAS"
else 
	echo "settimana non adatta per Elaborazione pc36" >> "$LOG_FILE"
fi
