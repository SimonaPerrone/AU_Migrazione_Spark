#!/bin/bash
# data richiesta
if [ -z "$1" ]; then
  log "No request date specified. Using default:"
  REQUEST_DATE=$(date -d "$date -1 days" +"%Y-%m-%d")
else
  REQUEST_DATE=$1
fi

bash ${deploy.path.local}/runPubblicazioni.sh $REQUEST_DATE "P"
bash ${deploy.path.local}/runPubblicazioni.sh $REQUEST_DATE "Pein"
bash ${deploy.path.local}/runPubblicazioni.sh $REQUEST_DATE "PR"
bash ${deploy.path.local}/runPubblicazioni.sh $REQUEST_DATE "PRein"
bash ${deploy.path.local}/runPubblicazioni.sh $REQUEST_DATE "CA"
