#!/bin/bash
flusso="tgl"
echoerr() { printf "\033[0;33m%s\n\033[0m" "$*"; }

echo -e "Compactin Flusso: \e[30;48;5;82m$flusso\033[0m"
#hadoop fs -ls /user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_${flusso}_p | grep mese_comp | awk '{print $6" "$7","$8}' > /tmp/${flusso}_partition.dat
#last_compact_flusso=$(hive -S -e "select max(d_compact_${flusso}) as last_compact_${flusso} from cmg_gas.report_compact" | grep -v "^WARN" 2> /dev/null)
last_compact_flusso="2020-03-25 20:00:00"
last_compact_flusso_t=$(date -d "$last_compact_flusso" +%s) 
IFS=','
partition_work=/tmp/${flusso}_partitions_work.dat
echo "Last data Compact: ${last_compact_flusso}"
echo "Partitions: $(cat /tmp/${flusso}_partition.dat | wc -l)"

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

echo -e "\e[0;33mList Partitions:\n"$(cat $partition_work)"\nSize:" $(cat $partition_work | wc -l)"\e[0m"

IFS='='
while read p
do
   echo -e "\e[0;33mElaborate Partition:$p\e[0m"
#/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tgl_p/mese_comp=122018
   read -ra partition_w <<< "$p"
   partition_value=${partition_w[1]}
   partition_name="mese_comp"
  
   echo $partition_value
  
#./compating_v3.sh cmg_gas prt_cmg_tml_p_compact /user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tml_p_compact annomese=202002
   
done < $partition_work
