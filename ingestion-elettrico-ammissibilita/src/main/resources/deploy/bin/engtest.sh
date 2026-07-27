ANNO=2021
MESE=07
GIORNO=24
LOG_FILE="/mnt/isilonshare1_Parallelo/Software/EE/bin/logEE.txt"

# Pocedure sqoop per tabelle
./sqoop.sh &>> "$LOG_FILE"
if [ "$?" -ne 0 ];
then
  echo "Sqoop exited with error" &>> "$LOG_FILE"
  echo "1" > ${schedulazioni.path}/error.sqoop
else
  echo "Sqoop Success, start ammissibilità " &>> "$LOG_FILE"
  echo "0" > ${schedulazioni.path}/error.sqoop
  # Procedura Ammissibilità
  ./flusso-misure.sh -ia -g -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &>> "$LOG_FILE"
  echo "Exit status ammissibilità --> $?" &>> "$LOG_FILE"
fi

ERRORSQOOP=$(grep 1 ${schedulazioni.path}/error.sqoop | wc -l)
if [[ ERRORSQOOP ]]
then
  echo "Sqoop exited with error" &>> "$LOG_FILE"
else
  echo "Sqoop Success, start ammissibilità " &>> "$LOG_FILE"
  rm ${schedulazioni.path}/error.sqoop
  # Procedura Ammissibilità
  ./flusso-misure.sh -ia -G -SS --anno "$ANNO" --mese "$MESE" --giorno "$GIORNO" &>> "$LOG_FILE"
  echo "Exit status ammissibilità --> $?" &>> "$LOG_FILE"
fi