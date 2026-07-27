#!/bin/bash

#grep -rli 'insufficient memory' /home/silvia/schedulazioni/log_crontab/* | grep GAS
#grep -rli 'Cannot allocate memory' /home/silvia/schedulazioni/log_crontab/* | grep GAS
#grep -rli 'Tot files :0\|non decompresso' /home/silvia/schedulazioni/log_crontab/*  | grep -v 'PDO\|RFO\|ALTRI\|SMIS'

#grep -rli 'org.apache.spark.SparkException' /home/silvia/schedulazioni/log_crontab/* | grep -v 'PDO\|RFO\|ALTRI\|SMIS'
#grep -rli 'Exception' /home/silvia/schedulazioni/log_crontab/* | grep -v 'PDO\|RFO\|ALTRI\|SMIS'
#grep -rli 'insufficient memory' /home/silvia/schedulazioni/log_crontab/* | grep -v 'PDO\|RFO\|ALTRI\|SMIS'
grep -rli 'attendere la fine del processo' /home/silvia/schedulazioni/log_crontab/* | grep -v 'PDO\|RFO\|ALTRI\|SMIS'
grep -rli 'cancellazione manuale' /home/silvia/schedulazioni/log_crontab/* | grep -v 'PDO\|RFO\|ALTRI\|SMIS'
#grep -rli 'attenzione' /home/silvia/schedulazioni/log_crontab/* | grep -v 'PDO\|RFO\|ALTRI\|SMIS'


