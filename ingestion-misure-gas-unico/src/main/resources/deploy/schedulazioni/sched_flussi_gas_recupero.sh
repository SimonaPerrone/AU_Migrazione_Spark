#!/bin/bash

export PYTHONIOENCODING=utf8

# Parameter definition

CURDD=`date +%Y-%m-%d`

CURTMS=`date +%Y%m%d%H%M%S`
SCHEDULATION_LOG_FILE="${schedulation.log.path}/log_SCHEDULATION_GAS__$CURTMS"".txt"
UNZIP_LOG_FOLDER="${logs.root.path}"

echo "Avviato Processo di Unzip e Ingestion Flussi GAS" &>> ${SCHEDULATION_LOG_FILE}
echo "Timestamp di lancio del processo: `date`" &>> ${SCHEDULATION_LOG_FILE}
echo "Processo lanciato in modalità recupero puntuale"
echo "Processo lanciato in modalità recupero puntuale" &>> ${SCHEDULATION_LOG_FILE}
echo "*************************************************" &>> ${SCHEDULATION_LOG_FILE}

#****************************************************************************************

STD_DEPLOY_PATH=${deploy.path}

# Starting Decompression process

echo "Unzip Flussi GAS in corso..."
echo "Unzip Flussi GAS in corso..." &>> ${SCHEDULATION_LOG_FILE}
"$STD_DEPLOY_PATH"/spark-submit-gas-unzip.sh -F
unzip_result=$?

if [ $unzip_result -eq 0 ]
then

  echo "--> processo di Unzip Flussi GAS andato a buon fine. Log del job disponibili all'interno della cartella:" &>> ${SCHEDULATION_LOG_FILE}
  echo "$UNZIP_LOG_FOLDER" &>> ${SCHEDULATION_LOG_FILE}
  dd=`date`
  echo "Processo di Unzip Flussi GAS terminato: $dd" &>> ${SCHEDULATION_LOG_FILE}

  #****************************************************************************************

  echo "Ingestion Flussi GAS STANDARD:"
  echo "Ingestion Flussi GAS STANDARD:" &>> ${SCHEDULATION_LOG_FILE}

  STANDARD_FLOWS=(
      "TALStandard"
      "TMLStandard"
      "RMLStandard"
      "TGLStandard"
      "RGLStandard"
      "RSLStandard"
      "TAVStandard"
      "TASStandard"
      "RMVStandard"
      "TMVStandard"
      "FUIStandard"
      "A01Standard"
      "A40Standard"
      "D01Standard"
      "SM1Standard"
      "R01Standard"
      "A02Standard"
      "V01Standard"
      "SM2Standard"
      "M01Standard"
      "V02Standard"
      "AD2Standard"
      "AD2RStandard"
      "AD3Standard"
      "AD3RStandard"
      "AD4Standard"
      "AD4RStandard"
      "AD5Standard"
      "AD5RStandard"
      "S02Standard"
      "S40Standard"
      "R40Standard"
      "SWG1Standard"
      "FDDStandard"
      "D02Standard"
      "D01RStandard"
      "D02RStandard"
      "R01RStandard"
      "A40RStandard"
      "S40RStandard"
      "R40RStandard"
      "A01RStandard"
      "A02RStandard"
      "S02RStandard"
      "V01RStandard"
      "M01RStandard"
      "V02RStandard"
      "SM1RStandard"
      "SM2RStandard"
  )


  echo "Running IGMG flow..."
  "$STD_DEPLOY_PATH"/spark-submit-gas-ingestion.sh -f IGMG
  result=$?
  echo "IGMG -> exit code: $result" &>> ${SCHEDULATION_LOG_FILE}
  if [ $result -eq 0 ]
  then
    echo "Running IGMG Export Process..."
    EXPORT_IGMG_LOG_FILE="${logs.root.path}/log-export-igmg-$(date +%Y-%m-%d_%H:%M:%S).log"
    "$STD_DEPLOY_PATH"/sqoop/igmg_export_cdp.sh &>> ${EXPORT_IGMG_LOG_FILE}
    result=$?
    echo "IGMG Export Process -> exit code: $result" &>> ${SCHEDULATION_LOG_FILE}
  else
    echo "IGMG Export Process not run since the job exited with a non-zero code." &>> ${SCHEDULATION_LOG_FILE}
  fi


  for flow in "${STANDARD_FLOWS[@]}"; do
      echo "Running $flow flow..."
      "$STD_DEPLOY_PATH"/spark-submit-gas-ingestion.sh -f "$flow"
      result=$?
      echo "$flow -> exit code: $result" &>> ${SCHEDULATION_LOG_FILE}
  done


  # Aggregazione, pubblicazione e tracking dei report di ammissibilità txt dei flussi GAS STANDARD
  REPORTS_AMMISSIBILITA_LOG_FILE="${report.log.path}/log_REPORTS_AMMISSIBILITA_GAS__$(date +%Y%m%d%H%M%S).log"
  "$STD_DEPLOY_PATH"/report/produce_reports_ammissibilita_cdp.sh -f -p |& tee -a ${REPORTS_AMMISSIBILITA_LOG_FILE} &> /dev/null
  reports_result=${PIPESTATUS[0]}

  if [ $reports_result -eq 0 ]; then
    echo "-> Processo di produzione e tracking reports di ammissibilità conlcuso correttamente" &>> ${SCHEDULATION_LOG_FILE}
  else
    echo "-> Processo di produzione e tracking reports di ammissibilità fallito. Verifica il seguente file di log: ${REPORTS_AMMISSIBILITA_LOG_FILE}" &>> ${SCHEDULATION_LOG_FILE}
  fi


  echo "Procedura di acquisizione GAS STANDARD e IGMG con produzione reports ammissibilità terminata: $(date)" &>> ${SCHEDULATION_LOG_FILE}


  #****************************************************************************************

else

  echo "FAILURE: il processo di Unzip Flussi GAS è fallito, quindi l'intero processo di ingestion non sarà avviato." &>> ${SCHEDULATION_LOG_FILE}
  echo "Verifica il file di log presente nella cartella $UNZIP_LOG_FOLDER con timestamp $CURDD" &>> ${SCHEDULATION_LOG_FILE}
  echo "Exit code Unzip job: $unzip_result" &>> ${SCHEDULATION_LOG_FILE}
  exit 1

fi

echo "Procedura di reportistica finale GAS terminata: $(date)" &>> ${SCHEDULATION_LOG_FILE}
