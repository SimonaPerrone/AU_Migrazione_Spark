#!/bin/bash

#IL PRIMO PARAMETRO E' IL DATABASE

tabelle=("$@")

#tabelle=( \
#forniture \
#forniture_info \
#switch \
#fasce \
#gdm \
#RCU_POD_DISTR \
#)

in_import=true
num_import_ok=0
lenx=$(echo "${#tabelle[@]}")
len=$((($lenx -1)))


while $in_import; do 

num_import_ok=0

	for ((i = 1; i < ${#tabelle[@]}; i++))
	do
                db=${tabelle[0]}
                
		tabella=${tabelle[$i]}
		validateTable=$(hive -S --database "$db" -e "SHOW TABLES LIKE '$tabella'")
		str_s="WARN: The method class org.apache.commons.logging.impl.SLF4JLogFactory#release() was invoked."
		str_s2="WARN: Please see http:\/\/www.slf4j.org\/codes.html#release for an explanation."
		str_r=""
		validateTable=$(sed "s/${str_s}/${str_r}/g" <<<"$validateTable")
		validateTable=$(sed "s/${str_s2}/${str_r}/g" <<<"$validateTable")
		exists=false
		if [[ -z $validateTable ]]; then
			num_import_ok=$((num_import_ok))
			echo "tabella $tabella non trovata nel db $db"
		else
			num_import_ok=$((num_import_ok+1))
		fi

	done


sleep 15


echo "numero di tabelle trovate nel db $db : $num_import_ok"

if [ $((num_import_ok)) -eq $len ];then in_import=false; fi
  
done
