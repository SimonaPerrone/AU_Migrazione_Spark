#!/bin/bash
flusso=$1
table_path=$3
partition_name=$2
table=$4
echoerr() { printf "\033[0;33m%s\n\033[0m" "$*"; }
HDFSPATH=/user/hive/warehouse/au.db/misure_gas_au/cmg_gas

d_date=$(date '+%F %T')
echo -e "Compacting Flusso: \e[30;48;5;82m$flusso\033[0m"
hadoop fs -ls ${table_path}/${table} | grep ${partition_name} | awk '{print $6" "$7","$8}' > /tmp/${flusso}_partition.dat
last_compact_flusso=$(hive -S -e "select max(d_compact_${flusso}) as last_compact_${flusso} from cmg_gas.report_compact" | grep -v "^WARN" 2> /dev/null)
last_compact_flusso_t=$(date -d "$last_compact_flusso" +%s) 
IFS=','
partition_work=/tmp/${flusso}_partitions_work.dat
echo "Last data Compact: ${last_compact_flusso}"
echo "Partitions: $(cat /tmp/${flusso}_partition.dat | wc -l)"

flusso_col=$(echo "${flusso}" | tr '[:upper:]' '[:lower:]')

new_path=$(date '+%Y%m%d')_prt_cmg_${flusso_col}_p
echo "hadoop fs -mkdir ${HDFSPATH}/${new_path}"
hadoop fs -mkdir ${HDFSPATH}/${new_path}

echo
echo -e "\e[30;48;5;82m Backup tabella prt_cmg_${flusso_col}_p \033[0m"
echo "hadoop fs -cp ${HDFSPATH}/prt_cmg_${flusso_col}_p/* ${HDFSPATH}/${new_path}"
hadoop fs -cp ${HDFSPATH}/prt_cmg_${flusso_col}_p/* ${HDFSPATH}/${new_path}

#exit
touch $partition_work > /dev/null
truncate -s 0 $partition_work
while read p
do
   read -ra LINE <<< "$p"
   #echo ${LINE}
   timestamp=$(date -d ${LINE[0]} +%s)
   path_partition=${LINE[1]}

   if ((timestamp > last_compact_flusso_t))
   then
       #echoerr "[DEBUG] Timestamp: "$timestamp" "${LINE[0]}" "$path_partition
       echo -e "\e[0;33m[DEBUG] Timestamp: "$timestamp"\t"${LINE[0]}"\t"$path_partition"\033[0m"
       echo $path_partition >> $partition_work 
   fi

done < /tmp/${flusso}_partition.dat

size=$(cat $partition_work | wc -l)
echo -e "\e[0;33mList Partitions:\n"$(cat $partition_work)"\nSize:" ${size}"\e[0m"

index=1
IFS='='
while read p
do
   echo
   echo "**********************************************************************************************************************************************************"
   echo " Operazione (${index} / ${size})"
   index=$((index+1))
   echo -e "\e[0;33mElaborate Partition:$p\e[0m"

   read -ra partition_w <<< "$p"
   partition_value=${partition_w[1]}
  
   echo "Valore partizione: "$partition_value
   echo "./compating_v3.sh cmg_gas ${table} ${table_path} ${partition_name}=${partition_value}"
   ./compating_v3.sh cmg_gas ${table} ${table_path} ${partition_name}=${partition_value}

   
   echo "**********************************************************************************************************************************************************"
done < $partition_work
flusso_col=$(echo "${flusso}" | tr '[:upper:]' '[:lower:]')
echo "hive -e 'Insert into cmg_gas.report_compact(d_compact_${flusso_col}) values('${d_date}')'"
hive -e "Insert into cmg_gas.report_compact(d_compact_${flusso_col}) values('${d_date}')"

rm /tmp/${flusso}_partition.dat
rm $partition_work

echo "Fine"
