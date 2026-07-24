#!/bin/bash
BASEPATH="/mnt/isilonshare1/Software/GAS/"
BASEPATH_LOG="${BASEPATH}log"
CURDD=`date +%Y-%m-%d`

ANNO_C=$1
MESE_C=$2
GIORNO_C=$3

grep -rli 'insufficient memory' ${BASEPATH_LOG}/*  | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g"
grep -rli 'Cannot allocate memory' /mnt/isilonshare1/Software/GAS/log/* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g"
grep -rli "Input path does not exist" ${BASEPATH_LOG}/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g"
grep -rli 'Traceback' ${BASEPATH_LOG}/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "ERROR,"$0",DATE"}' | sed "s|DATE|${CURDD}|g"
grep -rli "ATTENZIONE" ${BASEPATH_LOG}/log_*_GAS__${ANNO_C}${MESE_C}${GIORNO_C}* | awk '{print "WARN,"$0",DATE"}' | sed "s|DATE|${CURDD}|g"