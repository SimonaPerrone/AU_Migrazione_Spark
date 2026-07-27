echo "run ccg process..."
DATA_RICHIESTA=$(date +'%Y-%m-%d' -d "1 day ago")
DATE=$(date +'%Y-%m-%d_%H:%M:%S')
TIPO="ALL"
flowTypeName="ALL"
errorMessage=0
while getopts ":e:s:t:" opt; do
  case $opt in
    e) DATA_RICHIESTA="$OPTARG"
    ;;
    s) flowTypeName="${OPTARG:-$flowTypeName}"
    ;;
    t) TIPO="${OPTARG:-$TIPO}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

echo "data richiesta: $DATA_RICHIESTA"
echo "tipo richiesta: $TIPO"
echo "processo: $flowTypeName"


if [[(${errorMessage} -eq 0 && (${TIPO} = "ALL" || ${TIPO} = "PDR"))]];
then
  echo "run sqoop import CCG_RICHIESTA_PDR..."
  ${deploy.path}/sqoop-import-request-pdr.sh -e $DATA_RICHIESTA >> ${deploy.path}/logs/ccg-sqoop-import-pdr-$DATE.log
  errorMessage=$?
  echo "end sqoop import CCG_RICHIESTA_PDR"
fi

if [[(${errorMessage} -eq 0 && (${TIPO} = "ALL" || ${TIPO} = "FILTRO"))]];
then
  echo "run sqoop import CCG_RICHIESTA_FILTRO..."
  ${deploy.path}/sqoop-import-request-filtro.sh -e $DATA_RICHIESTA >> ${deploy.path}/logs/ccg-sqoop-import-filtro-$DATE.log
  errorMessage=$?
  echo "end sqoop import CCG_RICHIESTA_FILTRO"
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Sqoop import exited with error"
else
  echo "Sqoop import Success, start ccg publication..."

  if [[(${flowTypeName} = "ALL" || ${flowTypeName} = "AGG")]];
  then
    echo "Run CCG AGG..."
    ${deploy.path}/run_ccg_specific_session.sh -s AGG -d 3 -r 30G -n 60 -c 3 -m 30G -x 3G -e $DATA_RICHIESTA -h $DATE -t $TIPO
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
    then
      ${deploy.path}/sqoop-export-esito.sh >> ${deploy.path}/logs/ccg-sqoop-export-agg-$DATE.log
    else
      echo "CCG AGG exited with error"
    fi
    echo "End CCG AGG"
  fi

  if [[(${flowTypeName} = "ALL" || ${flowTypeName} = "SBG")]];
  then
    echo "Run CCG SBG..."
    ${deploy.path}/run_ccg_specific_session.sh -s SBG -d 3 -r 15G -n 20 -c 3 -m 15G -x 3G -e $DATA_RICHIESTA -h $DATE -t $TIPO
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
    then
      ${deploy.path}/sqoop-export-esito.sh >> ${deploy.path}/logs/ccg-sqoop-export-sbg-$DATE.log
    else
      echo "CCG SBG exited with error"
    fi
    echo "End CCG SBG"
  fi

  if [[(${flowTypeName} = "ALL" || ${flowTypeName} = "CDP_FIN")]];
  then
    echo "Run CCG CDP_FIN..."
    ${deploy.path}/run_ccg_specific_session.sh -s CDP_FIN -d 3 -r 15G -n 20 -c 3 -m 15G -x 3G -e $DATA_RICHIESTA -h $DATE -t $TIPO
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
    then
      ${deploy.path}/sqoop-export-esito.sh >> ${deploy.path}/logs/ccg-sqoop-export-cdp-fin-$DATE.log
    else
      echo "CCG CDP_FIN exited with error"
    fi
    echo "End CCG CDP_FIN"
  fi

  if [[(${flowTypeName} = "ALL" || ${flowTypeName} = "CDP_RIC")]];
  then
    echo "Run CCG CDP_RIC..."
    ${deploy.path}/run_ccg_specific_session.sh -s CDP_RIC -d 3 -r 15G -n 20 -c 3 -m 15G -x 3G -e $DATA_RICHIESTA -h $DATE -t $TIPO
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
    then
      ${deploy.path}/sqoop-export-esito.sh >> ${deploy.path}/logs/ccg-sqoop-export-cdp-ric-$DATE.log
    else
      echo "CCG CDP_RIC exited with error"
    fi
    echo "End CCG CDP_RIC"
  fi

echo "run remove file in tmp folder"
rm -r ${isilon.basepath.tmp}/tmp/CCG
echo "end remove file in tmp folder"

fi