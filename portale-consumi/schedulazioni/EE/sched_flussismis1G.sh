#!/bin/bash

BASEPATH="/home/silvia"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=`expr $ANNOS + 0`
MESES=`date -d "$DD" +%m`
MESE=`expr $MESES + 0`
GIORNOS=`date -d "$DD" +%d`
GIORNO=`expr $GIORNOS + 0`
CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="$BASEPATH""/schedulazioni/log_crontab/log_SMIS1G_$CURTMS"".txt"

echo " `date` - flusso-misure.sh -Di -g -SS --anno $ANNO --mese $MESE --giorno $GIORNO" >> "$BASEPATH/schedulazioni/out_sched.log"


cd /home/silvia/last_release/bin
sudo chown silvia:silvia /home/silvia/last_release/log/*

sudo ./flusso-misure.sh -D -g -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &> "$LOG_FILE"

error=$(grep -rli 'org.apache.spark.SparkException' "$LOG_FILE")
error="$error"$(grep -rli 'Exception' "$LOG_FILE")
error="$error"$(grep -rli 'insufficient memory' "$LOG_FILE")
error="$error"$(grep -rli 'attendere la fine del processo' "$LOG_FILE")
error="$error"$(grep -rli 'cancellazione manuale' "$LOG_FILE")

if [ -n "$error" ]
then
 echo "Decompressione FLUSSI SMIS 1G non andata a buon fine" &>> "$LOG_FILE"
 exit 1
fi

#cd "$BASEPATH/last_release/bin"
sudo chown silvia:silvia "$BASEPATH"/last_release/log/*

./flusso-misure.sh -i -g -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &>> "$LOG_FILE"

