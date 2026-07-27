#!/bin/bash

par1=$1
par2=$2
replacement=""
filecsv_EE=""
filecsv_GAS=""
o_ee=out_ee.log
o_gas=out_gas.log

if [[ $par1 =~ "--csv_ee=" ]]
then
	filecsv_EE=${par1/--csv_ee=/$replacement}  
	echo "file csv storico elettrico : $filecsv_EE"
fi

if [[ $par2 =~ "--csv_gas=" ]]
then
	filecsv_GAS=${par2/--csv_gas=/$replacement}  
	echo "file csv storico gas : $filecsv_GAS"
fi

if [[ $par2 =~ "--csv_ee=" ]]
then
	filecsv_EE=${par2/--csv_ee=/$replacement}  
	echo "file csv storico elettrico : $filecsv_EE"
fi

if [[ $par1 =~ "--csv_gas=" ]]
then
	filecsv_GAS=${par1/--csv_gas=/$replacement}  
	echo "file csv storico gas : $filecsv_GAS"
fi



if [[ $filecsv_EE == "" &&  $filecsv_GAS == "" ]]
then
	echo "Non è stato indicato nessun file csv ne per il GAS ne per l'Elettrico"
	echo "uso ./test_storico.sh --csv_gas=filecsv_per_il_gas --csv_ee=filecsv_per_elettrico"
	exit
fi 

run_curl_EE(){
	  eval cod_fs_x="$1"
	  eval pod_x="$2"	
          start=$(date +%s)

	  echo  "$(date) - curl 172.16.17.245:80/api/GetStoricoLettureElettriche?codice_fiscale=$cod_fs_x&codice_pod=$pod_x" >> "./$o_ee"
	  curl "172.16.17.245:80/api/GetStoricoLettureElettriche?codice_fiscale=$cod_fs_x&codice_pod=$pod_x" >> "./$o_ee" 
	  end=$(date +%s)
	  SCS=$(( ($end - $start) ))
	  echo "$(date) fine curl 172.16.17.245:80/api/GetStoricoLettureElettriche?codice_fiscale=$cod_fs_x&codice_pod=$pod_x
	  TOTALE TEMPO IMPIEGATO IN SECONDI : $SCS " >> "./$o_ee"

}

read_EE () {

        eval filecsv="$1"

        while read -r line; do
         txt="$line"

         if [[ $txt != "" && $txt != "codice_fiscale;pod" ]]
         then
          dati=($(echo "$txt" | tr ';' '\n'))
          cod_fs=$(echo "${dati[0]}")
          pod=$(echo "${dati[1]}")
 	  run_curl_EE "\${cod_fs}" "\${pod}" &
	  sleep 2
	 fi

        done < "$filecsv"

}

run_curl_GAS(){
          eval cod_fs_y="$1"
          eval pdr_y="$2"
	  start=$(date +%s)

	  echo  "$(date) - curl 172.16.17.245:80/api/GetStoricoLettureGas?codice_fiscale=$cod_fs_y&codice_pdr=$pdr_y" >> "./$o_gas"
          curl "172.16.17.245:80/api/GetStoricoLettureGas?codice_fiscale=$cod_fs_y&codice_pdr=$pdr_y" >> "./$o_gas" 
	  end=$(date +%s)
          SCS=$(( ($end - $start) ))
          echo "$(date) fine curl 172.16.17.245:80/api/GetStoricoLettureGas?codice_fiscale=$cod_fs_y&codice_pdr=$pdr_y
          TOTALE TEMPO IMPIEGATO IN SECONDI : $SCS " >> "./$o_gas"
}

read_GAS () {

	eval filecsv="$1"
	
	while read -r line; do
	 txt="$line"

	 if [[ $txt != "" && $txt != "codice_fiscale;pdr" ]]
	 then
	  dati=($(echo "$txt" | tr ';' '\n'))
	  cod_fs=$(echo "${dati[0]}")
	  pdr=$(echo "${dati[1]}")
	  run_curl_GAS "\${cod_fs}" "\${pdr}" &
	  sleep 2
	 fi

	done < "$filecsv"

}

if [[ $filecsv_EE != "" ]]
then
	if [ ! -f "$filecsv_EE" ]
	then
		echo "Il file csv per l'elettrico $filecsv_EE  non esiste"
		exit
	fi
	echo "" > "./$o_ee" 
	read_EE "\${filecsv_EE}" & 
fi 

if [[ $filecsv_GAS != "" ]]
then
	if [ ! -f "$filecsv_GAS" ]
        then
                echo "Il file csv per il gas $filecsv_GAS  non esiste"
                exit
        fi
	echo "" > "./$o_gas"

	read_GAS "\${filecsv_GAS}" & 
fi 


