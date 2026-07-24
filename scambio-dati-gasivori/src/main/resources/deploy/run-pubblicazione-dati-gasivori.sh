#Defizione funzione di esecuzione sqoop import CC
run_sqoop_import_cc() {
  if [[(${errorMessage} -eq 0)]];
  then
    echo "Running sqoop import GASIVORI_FILIERA_CC..."
    cd $DEPLOY_PATH/sqoop
    ./sqoop-import-gasivori-cc.sh >> $DEPLOY_PATH/log/sqoop/sqoop-import-gasivori-cc-$DATE.log
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
      then
        echo "Sqoop import GASIVORI_FILIERA_CC ended successfully."
      else
        echo "Sqoop import GASIVORI_FILIERA_CC exited with error."
    fi
  fi
}
#Defizione funzione di esecuzione sqoop import CSEA
run_sqoop_import_csea() {
  if [[(${errorMessage} -eq 0)]];
  then
    echo "Running sqoop import GASIVORI_FILIERA_CSEA..."
    cd $DEPLOY_PATH/sqoop
    ./sqoop-import-gasivori-csea.sh >> $DEPLOY_PATH/log/sqoop/sqoop-import-gasivori-csea-$DATE.log
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
      then
        echo "Sqoop import GASIVORI_FILIERA_CSEA ended successfully."
      else
        echo "Sqoop import GASIVORI_FILIERA_CSEA exited with error."
    fi
  fi
}
#Defizione funzione di esecuzione sqoop import ID
run_sqoop_import_id() {
  if [[(${errorMessage} -eq 0)]];
  then
    echo "Running sqoop import GASIVORI_FILIERA_ID..."
    cd $DEPLOY_PATH/sqoop
    ./sqoop-import-gasivori-id.sh >> $DEPLOY_PATH/log/sqoop/sqoop-import-gasivori-id-$DATE.log
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
      then
        echo "Sqoop import GASIVORI_FILIERA_ID ended successfully."
      else
        echo "Sqoop import GASIVORI_FILIERA_ID exited with error."
    fi
  fi
}
#Defizione funzione di esecuzione sqoop import UDB
run_sqoop_import_udb() {
  if [[(${errorMessage} -eq 0)]];
  then
    echo "Running sqoop import GASIVORI_FILIERA_UDB..."
    cd $DEPLOY_PATH/sqoop
    ./sqoop-import-gasivori-udb.sh >> $DEPLOY_PATH/log/sqoop/sqoop-import-gasivori-udb-$DATE.log
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
      then
        echo "Sqoop import GASIVORI_FILIERA_UDB ended successfully."
      else
        echo "Sqoop import GASIVORI_FILIERA_UDB exited with error."
    fi
  fi
}
#Defizione funzione di esecuzione sqoop import UDD
run_sqoop_import_udd() {
  if [[(${errorMessage} -eq 0)]];
  then
    echo "Running sqoop import GASIVORI_FILIERA_UDD..."
    cd $DEPLOY_PATH/sqoop
    ./sqoop-import-gasivori-udd.sh >> $DEPLOY_PATH/log/sqoop/sqoop-import-gasivori-udd-$DATE.log
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
      then
        echo "Sqoop import GASIVORI_FILIERA_UDD ended successfully."
      else
        echo "Sqoop import GASIVORI_FILIERA_UDD exited with error."
    fi
  fi
}
#Defizione funzione di esecuzione sqoop import AMM
run_sqoop_import_amm() {
  if [[(${errorMessage} -eq 0)]];
  then
    echo "Running sqoop import GASIVORI_PERIMETRO_AMM..."
    cd $DEPLOY_PATH/sqoop
    ./sqoop-import-perimetro-amm.sh >> $DEPLOY_PATH/log/sqoop/sqoop-import-perimetro-amm-$DATE.log
    errorMessage=$?
    if [[(${errorMessage} -eq 0)]];
      then
        echo "Sqoop import GASIVORI_PERIMETRO_AMM ended successfully."
      else
        echo "Sqoop import GASIVORI_PERIMETRO_AMM exited with error."
    fi
  fi
}

### Inizio dello script ###
echo "Run pubblicazione dati gasivori..."

errorMessage=0

DATE=$(date +'%Y-%m-%d_%H:%M:%S')
PROPERTIES_PATH=${hdfs.deploy.path}/params.properties
DEPLOY_PATH=${isilon.deploy.path}
MODES="CC,CSEA,ID,UDD,UDB,AMM"

while getopts ":p:m:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    m) MODES="${OPTARG:-$MODES}" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done

MODES=${MODES^^}

while IFS=',' read -ra ADDR; do
  for i in "${ADDR[@]}"; do
    case $i in
    CC) run_sqoop_import_cc ;;
    CSEA) run_sqoop_import_csea ;;
    ID) run_sqoop_import_id ;;
    UDB) run_sqoop_import_udb ;;
    UDD) run_sqoop_import_udd ;;
    AMM) run_sqoop_import_amm ;;
    *) echo "ERROR: Invalid mode $i" ; errorMessage=1 ;;
    esac
  done
done <<< "$MODES"

if [[(${errorMessage} -eq 0)]];
then
  echo "Sqoop imports ended successfully, running Spark process..."
  $DEPLOY_PATH/spark-submit-pubblicazione-dati-gasivori.sh -p $PROPERTIES_PATH -m $MODES
else
  echo "Sqoop imports exited with error!"
fi

if [[(${errorMessage} -eq 0)]];
then
  echo "Process ended successfully."
else
  echo "Process exited with error!"
fi

echo "Removing files in tmp folder..."
rm -rf ${isilon.basepath.tmp}/tmp/TDG/TDG1/*
rm -rf ${isilon.basepath.tmp}/tmp/TDG/TDG2/*
rm -rf ${isilon.basepath.tmp}/tmp/TDG/TDG3/*
rm -rf ${isilon.basepath.tmp}/tmp/TDG/TDG4/*
rm -rf ${isilon.basepath.tmp}/tmp/TDG/TDG5/*
echo "Temporary files removed."