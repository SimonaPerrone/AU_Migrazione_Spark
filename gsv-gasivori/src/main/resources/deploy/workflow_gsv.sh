echo "GSV workflow started."

echo "Starting gsv aggregation..."
${ISILON_DEPLOY_PATH}/run-gsv.sh
errorMessage=$?

if [[(${errorMessage} -ne 0)]];
then
  echo "Gsv aggregation exited with error."
  exit 2
else
  echo "Gsv aggregation successfully, starting sqoop export..."
  ${ISILON_DEPLOY_PATH}/sqoop-export.sh
  errorMessage=$?
fi
