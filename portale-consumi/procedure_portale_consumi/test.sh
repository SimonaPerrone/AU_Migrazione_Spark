#!/bin/bash

fcheck="$1"

cicla=true

while $cicla; do

 if [ -f "$fcheck" ]; then
    cicla=false
 else 
    echo "$(date) - IN ATTESA COMPLETAMENTO GAS"
 fi   	

sleep 60
	
done

rm -f "$fcheck"

echo "$(date) - GAS COMPLETATO"

