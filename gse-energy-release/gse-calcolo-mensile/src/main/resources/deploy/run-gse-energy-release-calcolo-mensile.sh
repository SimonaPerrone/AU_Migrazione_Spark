#!/bin/sh

PROPERTIES_PATH=${hdfs.deploy.path}/params.properties
DATE=$(date +'%Y-%m-%d_%H:%M:%S')
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"
errorMessage=0

while getopts ":p:" opt; do
  case $opt in
    p) PROPERTIES_PATH="${OPTARG:-$PROPERTIES_PATH}" ;;
    \?) echo "Invalid option -$OPTARG" >&2 ;;
  esac
done

echo "Running GSE Energy Release - Calcolo Mensile..."

if [[(${errorMessage} -ne 0)]];
then
  echo "Something went wrong during the parsing of input parameters."
  echo "Gse energy release workflow exited with error."
  exit 2
else
  echo "Importing GSE_PERIMETRO_ER_EE..."
  $DEPLOY_PATH/sqoop/sqoop-import-gse-perimeter.sh &> $DEPLOY_PATH/logs/sqoop/gse-cm-sqoop-import-perimeter-$DATE.log
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Sqoop import of GSE_PERIMETRO_ER_EE exited with error, please check the log $DEPLOY_PATH/logs/sqoop/gse-cm-sqoop-import-perimeter-$DATE.log."
  echo "Gse energy release workflow exited with error."
  exit 2
else
  echo "GSE_PERIMETRO_ER_EE imported successfully, importing GSE_RICHIESTA_ER_M..."
  $DEPLOY_PATH/sqoop/sqoop-import-gse-monthly-requests.sh &> $DEPLOY_PATH/logs/sqoop/gse-cm-sqoop-import-monthly-requests-$DATE.log
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Sqoop import of GSE_RICHIESTA_ER_M exited with error, please check the log $DEPLOY_PATH/logs/sqoop/gse-cm-sqoop-import-monthly-requests-$DATE.log."
  echo "Gse energy release workflow exited with error."
  exit 2
else
  echo "GSE_RICHIESTA_ER_M imported successfully, starting Spark process..."
  $DEPLOY_PATH/spark-submit-gse-calcolo-mensile.sh -p $PROPERTIES_PATH &> $DEPLOY_PATH/logs/spark/gse-cm-calcolo-mensile-$DATE.log
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Spark process exited with error, please check the log $DEPLOY_PATH/logs/spark/gse-cm-calcolo-mensile-$DATE.log or in folder $DEPLOY_PATH/log."
  echo "Gse energy release workflow exited with error."
  exit 2
else
  echo "Spark process ended successfully, exporting GSE_AGGR_M_EXPORT..."
  $DEPLOY_PATH/sqoop/sqoop-export-gse-aggr-m.sh &> $DEPLOY_PATH/logs/sqoop/gse-cm-sqoop-export-aggr-m-$DATE.log
  errorMessage=$?
fi

if [[(${errorMessage} -ne 0)]];
then
  echo "Sqoop export of GSE_AGGR_M_EXPORT process exited with error, please check the log $DEPLOY_PATH/logs/sqoop/gse-cm-sqoop-export-aggr-m-$DATE.log."
  echo "Gse energy release workflow exited with error."
  exit 2
else
  echo "GSE energy release workflow ended successfully."
fi