#!/bin/bash


SOURCE=""
ANNO=2019
MESE=9


##FILE=/home/acutest/20190826_GAS/bin/input.in
PAT="/home/acutest/GAS"
#ILE="$PATH/bin/input.in"
#FILE_LOG=$(date  +"%y%m%d")

DATE=`date "+%Y%m%d"`

exit
LOG=${PATH}/logs/result_${DATE}.log

rm ${FILE} &> /dev/null

echo "Elabor. dir: $SOURCE/*/$ANNO/$MESE" >> "$LOG"
for f in `ls -d $SOURCE/*/$ANNO/$MESE`
do
        if [  "$(ls -A $f)" ];
        then
                #echo "$f" >> /home/acutest/20190826_GAS/bin/input.in
                echo "$f" >> ${FILE}
        fi
done



if test -f "$FILE"; then
        echo "Start" >> ${LOG}
        #./VPG.sh -i /home/acutest/20190826_GAS/bin/
        ./TDS.sh -i ${PATH}/bin/
else
        echo "File non presente $FILE"   >> ${LOG}
        echo "FILE non presente $FILE"
fi

#echo "Start" >> /home/acutest/20190826_GAS/logs/result_$(date  +"%y%m%d").log
#./TDS.sh -i /home/acutest/20190826_GAS/bin/

