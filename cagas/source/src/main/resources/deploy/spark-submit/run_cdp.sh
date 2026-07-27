session="CDP"

while getopts ":s:e:p:" opt; do
  case $opt in
    s) session="${OPTARG:-$session}"
    ;;
    e) SYS_DATE="-d ${OPTARG}"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

errorMessage=0
if [[(${errorMessage} -eq 0)]]
then
  echo "run freezer..."
  ${isilon.deploy.path}/run_freezer.sh -p ${hdfs.deploy.path.freezer}/params.properties -s $session
  errorMessage=$?
  echo "end freezer"
fi

if [[(${errorMessage} -eq 0)]]
then
  echo "run ca..."
  ${isilon.deploy.path}/run_ca.sh -p ${hdfs.deploy.path}/config.properties -s $session
  echo "end ca"
fi