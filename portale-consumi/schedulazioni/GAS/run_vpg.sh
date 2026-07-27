#!/bin/bash
SOURCE=$1
ANNO=$2
MESE=$3

FILE=/home/acutest/20190826_GAS/bin/input.in
#rm /home/acutest/20190826_GAS/bin/input.in
rm ${FILE} &> /dev/null

for f in `ls -d $SOURCE/*/$ANNO/$MESE`
do
        if [  "$(ls -A $f)" ]; 
	then 
		#echo "$f" >> /home/acutest/20190826_GAS/bin/input.in
		echo "$f" >> ${FILE}
	fi
done


if test -f "$FILE"; then
	echo "Start" >> /home/acutest/20190826_GAS/logs/result_$(date  +"%y%m%d").log
	./VPG.sh -i /home/acutest/20190826_GAS/bin/
else
	echo "File non presente $FILE"   >> /home/acutest/20190826_GAS/logs/result_$(date  +"%y%m%d").log
	echo "FILE non presente $FILE"
fi
