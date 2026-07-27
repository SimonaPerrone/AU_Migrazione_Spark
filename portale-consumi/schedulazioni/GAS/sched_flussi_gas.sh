#!/bin/bash

export PYTHONIOENCODING=utf8

#/home/silvia/schedulazioni/sched_flussitds_gas.sh  
/home/acutest/GAS/bin/tools/sched_flussitds_gas.sh  
/home/silvia/schedulazioni/sched_flussitfc_gas.sh
/home/silvia/schedulazioni/sched_flussi_VPG_gas.sh

#Flusso SW1
/home/silvia/schedulazioni/sched_flussi_gen.sh SW1

#Flusso FUI
/home/silvia/schedulazioni/sched_flussi_gen.sh FUI

#Flusso DEF
/home/silvia/schedulazioni/sched_flussi_gen.sh DEF

#Flusso RGL
/home/silvia/schedulazioni/sched_flussi_gen.sh RGL

#Flusso RML
/home/silvia/schedulazioni/sched_flussi_gen.sh RML

#Flusso TAL
/home/silvia/schedulazioni/sched_flussi_gen.sh TAL

#Flussso TAV
/home/silvia/schedulazioni/sched_flussi_gen.sh TAV

#Flusso TGL
/home/silvia/schedulazioni/sched_flussi_gen.sh TGL

#Flusso RSL
/home/silvia/schedulazioni/sched_flussi_gen.sh RSL

#FLUSSO RMV
/home/silvia/schedulazioni/sched_flussi_gen.sh RMV

#Flusso TML
/home/silvia/schedulazioni/sched_flussi_gen.sh TML

#Flusso TAS
/home/silvia/schedulazioni/sched_flussi_gen.sh TAS

#Flusso TMV
/home/silvia/schedulazioni/sched_flussi_gen.sh TMV

#Flusso IM1 
/home/silvia/schedulazioni/sched_flussi_gen_im.sh IM1

#Flusso A01
/home/silvia/schedulazioni/sched_flussi_gen_2.sh A01

#FLusso A40
/home/silvia/schedulazioni/sched_flussi_gen_2.sh A40

#Flusso D01
/home/silvia/schedulazioni/sched_flussi_gen_2.sh D01

#Flusso SM1
/home/silvia/schedulazioni/sched_flussi_gen_2.sh SM1

#Pulizia workspace
workspace="/mnt/isilonshare1/GAS_INJ/isilonshare_gas"
echo "Pulizia directory di lavoro: ${workspace}"
sudo rm -rf ${workspace}/*

#Ricerca e Verifica sui file TXT contenuti all'interno della directory /mnt/isilonshare_gas
BASEPATH_LOG="/home/silvia"
CURDD=`date +%Y-%m-%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
ANNO=`date -d "$DD" +%Y`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`

ANNO_C=`date -d "$CURDD" +%Y`
MESE_C=`date -d "$CURDD" +%m`
GIORNO_C=`date -d "$CURDD" +%d`


CURTMS=`date +%Y%m%d%H%M%S`
report_errors="/home/acutest/GAS/bin/tools/reports/reports/report_erros_${CURTMS}.csv"
report_warns="/home/acutest/GAS/bin/tools/reports/reports/report_warns_${CURTMS}.csv"

report_errors_hdfs="/user/silvia/au/misure_gas_au/cmg_gas/report_errors"
LOG_FILE="$BASEPATH_LOG""/schedulazioni/log_crontab/log_VERIFICHE_TXT_GAS__$CURTMS"".txt"
echo "Inizio ricerca e verifiche sui file TXT: ${CURDD}" >> ${LOG_FILE}
cd /home/acutest/GAS/bin/tools/reports/ >> ${LOG_FILE}
sudo ./recupero_txt.sh ${ANNO} ${MESE} ${GIORNO} &>> ${LOG_FILE}
echo "Fine ricerca e verifiche sui file TXT: ${CURDD}" >> ${LOG_FILE}
./genera_report.sh &>> ${LOG_FILE}
echo "Controllo errori sui file di logs" >> ${LOG_FILE}
/home/silvia/schedulazioni/gas_check_logs_error.sh | grep ${ANNO_C}${MESE_C}${GIORNO_C} | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_errors}
grep -rli "Input path does not exist" /home/silvia/schedulazioni/log_crontab/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_errors}
grep -rli 'Traceback' /home/silvia/schedulazioni/log_crontab/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_errors}
grep -rli "ATTENZIONE" /home/silvia/schedulazioni/log_crontab/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "WARN,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_warns}
grep -rli "attendere la fine del processo in corso e riprovare" /home/silvia/schedulazioni/log_crontab/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "WARN,"$0",DATE"}' | sed "s|DATE|${CURDD}|g" > ${report_warns}
hadoop fs -mkdir  ${report_errors_hdfs} >> ${LOG_FILE}
hadoop fs -put ${report_errors}  ${report_errors_hdfs} >> ${LOG_FILE}
hadoop fs -put ${report_warns}  ${report_errors_hdfs} >> ${LOG_FILE}
echo "Controllo errori completato" >> ${LOG_FILE}

