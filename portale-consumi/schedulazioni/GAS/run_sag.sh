
#!/bin/bash
SOURCE=$1
ANNO=$2
MESE=$3
#INPUT_TISG_DIR="/mnt/isilonshare1/TISG_SAG1/"
INPUT_TISG_DIR="/mnt/isilonshare1/20191029_TISG_SAG/"
WORKDIR="/mnt/isilonshare1/WORKDIR/"
INPUTDIR="/home/acu/AU/PY/bin/"
INPUTFILE="/home/acutest/20190826_GAS/bin/input.in"


#echo "" > /home/acutest/GAS/bin/input.in
rm $INPUTFILE
rm $WORKDIR*

#unzip -qq /mnt/isilonshare1/TISG_SAG1/*_SAG1_*.zip -d $WORKDIR &> /dev/null


for f in `ls -d $INPUT_TISG_DIR`
do
        if [  "$(ls -A $f)" ];
        then
                #echo "$f" >> /home/acutest/GAS/bin/input.in
                echo "$f" >> $INPUTFILE
        fi
done

echo "Start" >> /home/acutest/20190826_GAS/logs/result_$(date  +"%y%m%d").log
#./SAG1.sh -i $INPUTDIR
