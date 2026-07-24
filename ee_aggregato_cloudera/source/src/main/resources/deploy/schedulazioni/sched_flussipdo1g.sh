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
LOG_FILE="$BASEPATH""/schedulazioni/log_crontab/log_PDO1G_$CURTMS"".txt"

echo " `date` - flusso-misure.sh -Di -g -t --anno $ANNO --mese $MESE --giorno $GIORNO" >> "$BASEPATH/schedulazioni/out_sched.log"

cd /home/sii_misure/last_release/bin
sudo chown sii_misure:sii_misure /home/sii_misure/last_release/log/*

./flusso-misure.sh -D -g -t --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &> "$LOG_FILE"

error=$(grep -rli 'org.apache.spark.SparkException' "$LOG_FILE")
error="$error"$(grep -rli 'Exception' "$LOG_FILE")
error="$error"$(grep -rli 'insufficient memory' "$LOG_FILE")
error="$error"$(grep -rli 'attendere la fine del processo' "$LOG_FILE")
error="$error"$(grep -rli 'cancellazione manuale' "$LOG_FILE")

if [ -n "$error" ]
then
 echo "Decompressione FLUSSI PDO 1G non andata a buon fine" &>> "$LOG_FILE"
 exit 1
fi

sudo chown sii_misure:sii_misure "$BASEPATH"/last_release/log/*

./flusso-misure-pc2.sh -i -g -t --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &>> "$LOG_FILE"

#echo "
#INGESTIONE ALTERNATIVA SU FLUSSO_MISURE_QUARTI RIPARTIZIONATA
#" &>> "$LOG_FILE"

#cd /home/sii_misure/last_release/bin
#sudo ./flusso-misure.sh -i_new -G -t &>> "$LOG_FILE"

# Procedura Ammissibilità
./flusso-misure.sh -ia -g --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &>> "$LOG_FILE"
echo "Exit status ammissibilità --> $?" &>> "$LOG_FILE"


