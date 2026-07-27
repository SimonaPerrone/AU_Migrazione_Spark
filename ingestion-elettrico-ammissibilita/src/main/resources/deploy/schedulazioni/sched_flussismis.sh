#!/bin/bash

BASEPATH="/home/sii_misure"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNOS=`date -d "$DD" +%Y`
ANNO=`expr $ANNOS + 0`
MESES=`date -d "$DD" +%m`
MESE=`expr $MESES + 0`
GIORNOS=`date -d "$DD" +%d`
GIORNO=`expr $GIORNOS + 0`
CURTMS=`date +%Y%m%d%H%M%S`
LOG_FILE="$BASEPATH""/schedulazioni/log_crontab/log_SMIS2G_$CURTMS"".txt"

echo " `date` - flusso-misure.sh -Di -G -SS --anno $ANNO --mese $MESE --giorno $GIORNO" >> "$BASEPATH/schedulazioni/out_sched.log"


cd /home/sii_misure/last_release/bin
sudo chown sii_misure:sii_misure /home/sii_misure/last_release/log/*

#sudo ./flusso-misure.sh -D -G -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &> "$LOG_FILE"
./flusso-misure.sh -D -G -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &> "$LOG_FILE"

error=$(grep -rli 'org.apache.spark.SparkException' "$LOG_FILE")
error="$error"$(grep -rli 'Exception' "$LOG_FILE")
error="$error"$(grep -rli 'insufficient memory' "$LOG_FILE")
error="$error"$(grep -rli 'attendere la fine del processo' "$LOG_FILE")
error="$error"$(grep -rli 'cancellazione manuale' "$LOG_FILE")

if [ -n "$error" ]
then
 echo "Decompressione FLUSSI SMIS non andata a buon fine" &>> "$LOG_FILE"
 exit 1
fi

#cd "$BASEPATH/last_release/bin"
sudo chown sii_misure:sii_misure "$BASEPATH"/last_release/log/*

./flusso-misure.sh -i -G -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &>> "$LOG_FILE"

ERRORSQOOP=$(grep 1 ${schedulazioni.path}/error.sqoop | wc -l)
if [ ERRORSQOOP -ne 0 ]
then
  echo "Sqoop exited with error" &>> "$LOG_FILE"
else
  echo "Sqoop Success, start ammissibilità " &>> "$LOG_FILE"
  rm ${schedulazioni.path}/error.sqoop
  # Procedura Ammissibilità
  ./flusso-misure-ammissibilita.sh -n 55 -m 30G -G -S -Y "$ANNO" -M "$MESE" -D "$GIORNO" &>> "$LOG_FILE"
  echo "Exit status ammissibilità --> $?" &>> "$LOG_FILE"
fi