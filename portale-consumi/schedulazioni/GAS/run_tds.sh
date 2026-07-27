#!/bin/bash
SOURCE=$1
ANNO=$2
MESE=$3

#FILE=/home/acutest/20190826_GAS/bin/input.in
PATH_GAS=/home/acutest/GAS
FILE=${PATH_GAS}/bin/input.in
#FILE_LOG=$(date  +"%y%m%d")
DATE=`date "+%Y%m%d"`
LOG=${PATH_GAS}/logs/result_${DATE}.log

echo ${LOG}
rm ${FILE} &> /dev/null

echo "Elabor. dir: $SOURCE/*/$ANNO/$MESE" >> "$LOG"
for f in `ls -d $SOURCE/*/$ANNO/$MESE`
do
        if [  "$(ls -A $f)" ]; 
	then 
		echo "$f" >> ${FILE}
		#echo "File Elab. `ls $f`" 
	fi
done

if test -f "$FILE"; then
        echo "Start" >> ${LOG}
	./TDS.sh -i ${PATH_GAS}/bin/
else
        echo "File non presente $FILE"   >> ${LOG}
        echo "FILE non presente $FILE"
fi

#echo "Start" >> /home/acutest/20190826_GAS/logs/result_$(date  +"%y%m%d").log
#./TDS.sh -i /home/acutest/20190826_GAS/bin/
