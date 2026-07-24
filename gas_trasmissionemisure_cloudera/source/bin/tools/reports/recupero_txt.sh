#!/bin/bash

ANNO=$1
MESE=$2
GIORNO=$3

PATH_GAS="/mnt/isilonshare_gas/"
LOCAL_PATH="/mnt/isilonshare1/Software/GAS/bin/tools/reports/"
filtro="/DISTRIBUTORE/*/$ANNO/$MESE$GIORNO/"
file_report_all="${LOCAL_PATH}/list_files.log"

report="report.log"
echo $$ > recupero.pid

echo "Pulizia Directory"
rm ${LOCAL_PATH}files/*
truncate -s 0 ${file_report_all}

echo "Ricerca file TXT"
for i in $(ls $PATH_GAS)
do
   file_report="${LOCAL_PATH}files/${i}_${ANNO}${MESE}${GIORNO}_elenco_file.csv"
   echo "$file_report"
   touch $file_report 

   echo "find $PATH_GAS$i$filtro -type f | grep txt"
   #find $PATH_GAS$i$filtro -type f | grep txt 1>> $file_report 2> /dev/null
   find $PATH_GAS$i$filtro -type f 1>> $file_report_all 2> /dev/null

done


path_hdf="/user/hive/warehouse/au.db/misure_gas_au/cmg_gas/report_listfiles_tmp"

echo "Creo directory hdfs tmp"
hadoop fs -mkdir $path_hdf 2> /dev/null

echo "Pulizia directory hdfs tmp"
items_removed=$(hadoop fs -rm -skipTrash $path_hdf/* | wc -l)
echo "Elementi eliminati su hdfs: ${items_removed}" 

echo "Elimina file tmp"
rm ${LOCAL_PATH}files/*_list* 

count_files=$(ls  ${LOCAL_PATH}files/ | grep -v list | wc -l )
count=1

echo "Numero di file da copiare: ${count_files}"
for i in $(ls ${LOCAL_PATH}files/ | grep -v list)
do
  
  file_report_list="${LOCAL_PATH}files/${i}_${ANNO}${MESE}${GIORNO}_list.csv"
  echo "Size,Date,Hour,filename,filename_src" > $file_report_list
  #echo "Lettura file "${LOCAL_PATH}files/$i
  while read -r line
  do
     #echo "Lettura file 2 "$line 
     #cat $line | grep -v Elenco 
     size_file=$(cat $line | grep -v Elenco | sed '/^$/d' | wc -l)
     #echo "size: ${size_file}"
     if [ "${size_file}" -ne "0" ]; then
         #echo "copy ${size_file}"
         #report_line_out=$(cat $line | grep -v Elenco | sed '/^$/d' | awk '{print $1","$2" "$3","$4","substr($0, index($0,$5))",PATHLINE"}') # $file_report_list
         cat $line | grep -v Elenco | sed '/^$/d' | awk '{print $1","$2" "$3","$4","substr($0, index($0,$5))",PATHLINE"}' >> $file_report_list
         sed -i "s|PATHLINE|"$(dirname ${line})"|g"  $file_report_list

     fi
     

  done < ${LOCAL_PATH}files/$i
  #cat $(cat ${LOCAL_PATH}files/$i)
  #echo "hadoop fs -put $file_report_list $path_hdf"
  hadoop fs -put $file_report_list $path_hdf

  count=$((count+1))
  c=$(((count*100)/$count_files))
  echo -en "Copia in corso. COMPLETO: "$c"%" / $count_files"\0015"

done

hadoop fs -chown -R sii_misure_gas $path_hdf
hadoop fs -chown -R sii_misure_gas /user/hive/warehouse/au.db/misure_gas_au/cmg_gas/report_listfiles 

query_insert="set hive.exec.dynamic.partition.mode=nonstrict;INSERT INTO cmg_gas.report_file PARTITION(dataelaborazione) SELECT size,datacreazione,ora,filename,filename_src,current_date as dataelaborazione FROM cmg_gas.report_file_t;"
echo "Esegue inserimento in tabella parquet"
echo ${query_insert}

hive -e "${query_insert}"

echo "Pulizia Directory"
rm ${LOCAL_PATH}files/*
