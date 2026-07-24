echo "Run Aggregatore CDP process..."
DATE=$(date +'%Y%m%d%H%M%S')
PROPERTIES=${params.config.path}

while getopts ":p:" opt; do
  case $opt in
    p) PROPERTIES="${OPTARG:-$PROPERTIES}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

echo "PROPERTIES file: $PROPERTIES"
echo "DEPLOY PATH: ${isilon.deploy.path}"

echo "Creating CDP files..."
${isilon.deploy.path}/produzione-file-cdp.sh -p $PROPERTIES >> ${isilon.deploy.path}/nohup_logs/aggregatore_consumi_cdp_$DATE.out
errorMessage=$?
if [[(${errorMessage} -eq 0)]];
then
  echo "CDP files created successfully, starting sqoop export process..."
  ${isilon.deploy.path}/exportCdpDatiPrelievoGas.sh >> ${isilon.deploy.path}/nohup_logs/sqoop-export-cdp_$DATE.out
  errorMessage=$?
  if [[(${errorMessage} -ne 0)]];
  then
    echo "Sqoop export exited with error."
  else
    echo "CDP.CDP_DATI_PRELIEVO_GAS exported successfully."
  fi
else
  echo "CDP file creation process exited with error."
fi
echo "End Aggregatore CDP process."