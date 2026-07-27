#!/bin/bash

per=$(cat /home/leonardo/AU_EE/portale_consumi/set_periodo/.periodo)

if [[ (! -v per) || -z "$per" ]];then
 per=1126
 echo "Periodo massimo non impostato. Impostazione a 36 mesi di default"
fi

echo $per

