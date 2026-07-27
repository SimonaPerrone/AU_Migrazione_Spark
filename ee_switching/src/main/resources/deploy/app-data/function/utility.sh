export LOG_PATH='${logs.root.path}/calcolo-storici'
export LOG_FILE_NAME="$(date '+%Y-%m-%d_%H-%M-%S').log"
export LOG_FILE="${LOG_PATH}/${LOG_FILE_NAME}"
mkdir -p "${LOG_PATH}"
touch "${LOG_FILE}"

log(){
    local log_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${log_timestamp}] - $*"
    echo "[${log_timestamp}] - $*" >> "${LOG_FILE}"
}
die(){
    log "$*"
    exit 2
}
file_exists() {
  FILE=$1
  if ! [[ -f "$FILE" ]]; then
    die "Bad file_path : $FILE does not exist."
  fi
}
convert_file_to_array_string() {
  FILE=$1
  cat $FILE |
    #rimuovo eventuali linee vuote
    sed '/^$/d' |
    #aggiungo alla fine di ogni riga la virgola
    sed 's/$/,/g' |
    #trasformo n linee in una sola
    tr -d '\n' |
    #elimino la virgola alla fine della string
    sed 's/,$//g' |
    #aggiungo le virgolette
    sed "s/,/','/g" |
    #aggiungo la virgola all'inizio e alla fine
    sed "s/^/'/g" | sed "s/$/'/g"
}
get_first_data_curr_month() {
  date -d"1 $(date +%b)" +%F
}
get_first_data_next_curr_month() {
  date -d "1 $(date -d "1 $(date +%b) next month" +%b)" +%F
}
is_pair() {
  DECORRENZA_DATES=$1
  IFS=','
  #Read the split words into an array based on space delimiter
  read -a var <<<"$DECORRENZA_DATES"
  nvar=${#var[*]}
  if [[ nvar -eq 2 ]]; then
    echo "2"
  elif [[ nvar -eq 1 ]]; then
    echo "1"
  else
    die "Bad num param: ${DECORRENZA_DATES} can be one value or pair values"
  fi
}
check_timestamp() {
  timestamp=$1
  if ! [[ "$(date -d "${timestamp}" '+%Y-%m-%d %H:%M:%S' 2>/dev/null)" == "${timestamp}" ]]; then
    die "Bad switching date: ${timestamp} does not have the '+%Y-%m-%d %H:%M:%S' format."
  else
    echo $timestamp
  fi

}
check_switching_dates() {
  if [[ -v STORICI_SWITCHING_DATES ]]; then
    STORICI_SWITCHING_DATES_SEPARATORS="${STORICI_SWITCHING_DATES//[^,]/}"
    STORICI_SWITCHING_DATE_ARRAY=($(echo $STORICI_SWITCHING_DATES | tr ',' ' '))
    STORICI_SWITCHING_DATES_NUM_ELEMENTS="${#STORICI_SWITCHING_DATE_ARRAY[@]}"
    if [[ "${STORICI_SWITCHING_DATES_NUM_ELEMENTS}" -gt 2 ]]; then
      die "Bad storici switching dates: added more than 2 dates in the array."
    fi
    for switching_date in "${STORICI_SWITCHING_DATE_ARRAY[@]}"; do
      if ! [[ "$(date -d "${switching_date}" '+%Y%m%d' 2>/dev/null)" == "${switching_date}" ]]; then
        die "Bad switching date: ${switching_date} does not have the '%Y%m%d' format."
      fi
    done
    if [[ "${STORICI_SWITCHING_DATES_NUM_ELEMENTS}" == "2" ]]; then
      if [[ "${STORICI_SWITCHING_DATE_ARRAY[0]:6:2}" != "01" || "${STORICI_SWITCHING_DATE_ARRAY[1]:6:2}" != "01" ]]; then
        die "Bad storici switching dates: ${STORICI_SWITCHING_DATES} should be the first of the month for both dates."
      fi
      if [[ $(date -d "${STORICI_SWITCHING_DATE_ARRAY[0]} + 1 months" '+%Y%m%d') != "${STORICI_SWITCHING_DATE_ARRAY[1]}" ]]; then
        die "Bad storici switching dates: ${STORICI_SWITCHING_DATES} should be two consecutive months."
      fi
    fi
    echo "${STORICI_SWITCHING_DATE_ARRAY[0]},${STORICI_SWITCHING_DATE_ARRAY[1]}"
  fi
}
get_param_of_pair() {
  separator=$1
  str=$2
  param=$3
  array=($(echo $str | tr $separator ' '))
  echo ${array[$param]}
}
needs_arg() {
  if [ -z "$OPTARG" ]; then
    die "No arg for --$OPT option"
  fi
}
print_help() {
  log "ee-switching-dati-funzionali.sh"
  log "                                [-h|--help]"
  log "                                [-v|--verbose]"
  log "                                [-t|--path-timestamp=/dir1/dir2/filename.txt"
  log "                                [-p|--path-pod-list=/dir1/dir2/filename.txt"
  log "                                [-s|--path-date-switching=20200101[,20200201,...]"
  log "                                [-d|--path-piva-distributore-list=/dir1/dir2/filename.txt"
  log "                                [-D|--path-piva-udd-list=/dir1/dir2/filename.txt"
  log "                                [-c|--path-pair-piva-udd-piva-distr-list=/dir1/dir2/filename.txt"
  log "                                [-dbg|--debug"

}
check_combination_input_param() {
  # Se viene specificato un timestamp allora non sono ammessi altri parametri
  if [ -v FILE_INGESTION_TIMESTAMP ] && ([ -v FILE_PODS ] || [ -v FILE_STORICI_SWITCHING_DATES ] || [ -v FILE_PIVAS_DISTR ] || [ -v FILE_PIVAS_UDD ] || [ -v FILE_PAIR_PIVAS_UDD_PIVA_DISTR ]); then
    die "Bad storici param : you cannot pass -t and ( any param)"
  fi
  # Se vengono specificate delle date non è possibile fornire contemporaneamente liste di pod, distributori e udd
  if [ -v FILE_STORICI_SWITCHING_DATES ] && ([ -v FILE_PODS ] && [ -v FILE_PIVAS_DISTR ] && [ -v FILE_PIVAS_UDD ]); then
    die "Bad storici param : you cannot pass -s and ( -p and -d and -D )"
  fi
  # Se vengono specificate delle date non è possibile fornire contemporaneamente liste di distributori e udd
  if [ -v FILE_STORICI_SWITCHING_DATES ] && ([ -v FILE_PIVAS_DISTR ] && [ -v FILE_PIVAS_UDD ]); then
    die "Bad storici param : you cannot pass -s and (-d and -D )"
  fi
  # Se vengono specificate delle date non è possibile fornire contemporaneamente liste di pod e distributori")
  if [ -v FILE_STORICI_SWITCHING_DATES ] && ([ -v FILE_PODS ] && [ -v FILE_PIVAS_DISTR ]); then
    die "Bad storici param : you cannot pass -s and (-p and -d )"
  fi
  # Se vengono specificate delle date non è possibile fornire contemporaneamente liste di pod e udd")
  if [ -v FILE_STORICI_SWITCHING_DATES ] && ([ -v FILE_PODS ] && [ -v FILE_PIVAS_UDD ]); then
    die "Bad storici param : you cannot pass -s and (-p and -D )"
  fi
  # Se vengono specificate delle coppie distributore,udd non è possibile fornire contemporaneamente liste di pod, distributori o udd")
  if [ -v FILE_PAIR_PIVAS_UDD_PIVA_DISTR ] && ([ -v FILE_PODS ] || [ -v FILE_PIVAS_UDD ] || [ -v FILE_PIVAS_DISTR ]); then
    die "Bad storici param : you cannot pass -c and (-p or -d or -D)"
  fi
  if [ -v FILE_INGESTION_TIMESTAMP ] && [ -v FILE_STORICI_SWITCHING_DATES ]; then
    die "Bad storici param : you cannot pass -t and -s"
  fi
}
create_var() {
  check_combination_input_param

  if [[ ${PRINT_HELP} ]]; then
    print_help
    exit 0
  fi
  if [[ -v FILE_INGESTION_TIMESTAMP ]]; then
    file_exists $FILE_INGESTION_TIMESTAMP
    INGESTION_TIMESTAMP=$(cat $FILE_INGESTION_TIMESTAMP)
    INGESTION_TIMESTAMP=$(check_timestamp "$INGESTION_TIMESTAMP")
    if ! [[ -v DEBUG ]]; then
      result=$(beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' --outputformat=csv2 --showHeader=false -e "$(cat "${dir_query}/get_data_decorrenza_for_timestamp.sql")" $HIVE_VAR -hiveconf timestamp_par="${INGESTION_TIMESTAMP}" 2> "${LOG_FILE}")
    else
      result=">>>DATA-SINGOLA:2020-12-01 2021-01-01<<<"
    fi
    #get data between >>>data<<<
    result2=$(echo $result | sed 's/^[^>>>]*>>>//' | sed 's/<<<.*$//')
    if [[ "${result2}" == "DATA-SINGOLA:"* ]]
    then
      log "timestamp format ok"
      FIRST_SWITCHING_DATA=$(date -d "${result2:13:10}" '+%Y%m%d')
      SINGLE_SWITCHING_DATA=$(date -d "${result2:24:10}" '+%Y%m%d')
    elif [[ "${result2}" == "DATA-DOPPIA:"* ]]
    then
      log "timestamp format ok"
      FIRST_SWITCHING_DATA=$(date -d "${result2:12:10}" '+%Y%m%d')
      SECOND_SWITCHING_DATA=$(date -d "${result2:23:10}" '+%Y%m%d')
    else
      die "Bad timestamp : ${INGESTION_TIMESTAMP} does not exist."
    fi
    log "By ${INGESTION_TIMESTAMP} I recovered ${result2}"
  fi

  if [[ -v FILE_PODS ]]; then
    file_exists $FILE_PODS
    POD_ARRAY=$(convert_file_to_array_string $FILE_PODS)
  fi
  if [[ -v FILE_PIVAS_DISTR ]]; then
    file_exists $FILE_PIVAS_DISTR
    PI_DISTRIBUTORE_ARRAY=$(convert_file_to_array_string $FILE_PIVAS_DISTR)
  fi

  if [[ -v FILE_PIVAS_UDD ]]; then
    file_exists $FILE_PIVAS_UDD
    PIVAS_UDD_ARRAY=$(convert_file_to_array_string $FILE_PIVAS_UDD)
  fi
  if [[ -v FILE_STORICI_SWITCHING_DATES ]]; then
    file_exists $FILE_STORICI_SWITCHING_DATES
    STORICI_SWITCHING_DATES=$(convert_file_to_array_string $FILE_STORICI_SWITCHING_DATES | sed "s/'/ /g")
    log "STORICI_SWITCHING_DATES: $STORICI_SWITCHING_DATES"
    log "********************************************"
    log "I WILL NOT ORDINARY CALL OF FLOW"
    log "********************************************"
    result=$(check_switching_dates)
    flag_is_pair=$(is_pair $result)
    if [[ flag_is_pair -eq 2 ]]; then
      FIRST_SWITCHING_DATA=$(get_param_of_pair ',' $result 0)
      SECOND_SWITCHING_DATA=$(get_param_of_pair ',' $result 1)
    else
      SINGLE_SWITCHING_DATA=$(echo $result | sed 's/,//g')
      SINGLE_SWITCHING_DATA_MONTH_START=$(date -d "$SINGLE_SWITCHING_DATA" '+%Y%m01')
      FIRST_SWITCHING_DATA=$(date -d "$SINGLE_SWITCHING_DATA_MONTH_START -1 month" '+%Y%m%d')
    fi
  fi
  #CHIAMATA ORDINARIA
  if ! [ -v FILE_STORICI_SWITCHING_DATES ] && ! [ -v FILE_INGESTION_TIMESTAMP ]; then
    log "********************************************"
    log "I WILL ORDINARY CALL OF FLOW"
    log "********************************************"
    complete_first_data=$(get_first_data_curr_month)
    complete_second_data=$(get_first_data_next_curr_month '+%Y%m%d')
    FIRST_SWITCHING_DATA=$(date -d "$complete_first_data" '+%Y%m%d')
    SECOND_SWITCHING_DATA=$(date -d "$complete_second_data" '+%Y%m%d')
  fi
  if [[ -v FILE_PAIR_PIVAS_UDD_PIVA_DISTR ]]; then
    file_exists $FILE_PAIR_PIVAS_UDD_PIVA_DISTR
    string=$(cat $FILE_PAIR_PIVAS_UDD_PIVA_DISTR | tr '\n' '|')
    IFS='|' read -r -a PAIR_PIVAS_UDD_PIVA_DISTR_ARRAY <<<"$string"
  fi
}
create_check_pod() {
  if [[ -v POD_ARRAY ]]; then
    CHECK_POD="pod14 in ($POD_ARRAY)"
  else
    CHECK_POD="na"
  fi
}
create_check_piva_distributore() {
  if [[ -v PI_DISTRIBUTORE_ARRAY ]]; then
    CHECK_PIVA_DISTR="PIVA_DISTR in ($PI_DISTRIBUTORE_ARRAY)"
  else
    CHECK_PIVA_DISTR="na"
  fi
}
create_check_pair_piva_dist_piva_udd() {
  if [[ -v PAIR_PIVAS_UDD_PIVA_DISTR_ARRAY ]]; then
    for pair in "${PAIR_PIVAS_UDD_PIVA_DISTR_ARRAY[@]}"; do
      IFS=',' read -a pairT <<<"$pair"
      CHECK_PAIR_PIVA_DIST_PIVA_UDD+="(PIVA_DISTR = '${pairT[0]}' and PIVA_UDD = '${pairT[1]}') or "
    done
    CHECK_PAIR_PIVA_DIST_PIVA_UDD=$(echo "($CHECK_PAIR_PIVA_DIST_PIVA_UDD)" | sed 's/or )/)/g')
  else
    CHECK_PAIR_PIVA_DIST_PIVA_UDD="na"
  fi
}
create_check_udd() {
  if [[ -v PIVAS_UDD_ARRAY ]]; then
    CHECK_PIVA_UDD="PIVA_UDD in ($PIVAS_UDD_ARRAY)"
  else
    CHECK_PIVA_UDD="na"
  fi
}
create_check_switching_dates() {
  if [ -v FIRST_SWITCHING_DATA ] && [ -v SECOND_SWITCHING_DATA ]; then
    CHECK_ANNO_MESE="(annomese_sw = '${FIRST_SWITCHING_DATA:0:6}' or annomese_sw = '${SECOND_SWITCHING_DATA:0:6}')"
    CHECK_DATA="'$FIRST_SWITCHING_DATA','yyyyMMdd'"
    CHECK_SINGLE_DATA_DECORRENZA="na"
  elif [[ -v SINGLE_SWITCHING_DATA ]]; then
    CHECK_ANNO_MESE="annomese_sw = '${SINGLE_SWITCHING_DATA:0:6}'"
    CHECK_DATA="'$FIRST_SWITCHING_DATA','yyyyMMdd'"
    CHECK_SINGLE_DATA_DECORRENZA="d_data_decorrenza = '$(date -d "$SINGLE_SWITCHING_DATA" '+%F')'"
  else
    CHECK_ANNO_MESE="na"
    CHECK_DATA="na"
    CHECK_SINGLE_DATA_DECORRENZA="na"
  fi
}
create_check_holidays() {
  tmp_dir="$1"
  query_file="$2"
  script_dir="$3"
  curr_date=$(date -d "${FIRST_SWITCHING_DATA}" '+%Y')
  first_date=$(date -d "${FIRST_SWITCHING_DATA} -1 year" '+%Y')
  second_date=$(date -d "${FIRST_SWITCHING_DATA} -2 year" '+%Y')
  log "FIRST_SWITCHING_DATA:${FIRST_SWITCHING_DATA} => Holidays year: ${curr_date}, ${first_date} and ${second_date}"
  if ! [[ -v DEBUG ]]; then
    years=($curr_date $first_date $second_date)
    rm -rf "${tmp_dir}"
    for y in "${years[@]}"; do
      tmp_year_dir="$tmp_dir/$y"
      mkdir -p $tmp_year_dir
      #parallel execution
      beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' --outputformat=csv2 --showHeader=false -e "$(cat "${query_file}")" -hiveconf year="${y}" > "${tmp_year_dir}/output.csv" &
    done
    wait

    holidaysFilename="holidaysDateList.txt"
    for y in "${years[@]}"; do
      tmp_year_dir="${tmp_dir}/${y}"
      cat ${tmp_year_dir}/* >> "${tmp_year_dir}/${holidaysFilename}"
      file_exists "${tmp_year_dir}/${holidaysFilename}"
      year_holiday=$(convert_file_to_array_string "${tmp_year_dir}/${holidaysFilename}")
      CHECK_HOLIDAYS_ARRAY+=("${year_holiday[@]}")
    done
    CHECK_HOLIDAYS=$(echo "${CHECK_HOLIDAYS_ARRAY[@]}" | tr " " ",")
  else
    file_holidays="${script_dir}/app-data/file_input/holidays_list.txt"
    CHECK_HOLIDAYS=$(convert_file_to_array_string $file_holidays)
  fi
}
create_check_purge_view_sw_con_tra_view_rs() {
  file_sql=$1
  source $file_sql
  CHECK_PURGE_VIEW_SW_CON_TRA_VIEW_RS=$query
}
create_added_view_purged() {
  file_sql=$1
  source $file_sql
  CHECK_ADDED_VIEW_PURGED=$query
}
create_map_reduce_param() {
MAP_REDUCE_PARAM=$(cat <<- END
SET mapreduce.map.memory.mb=7000; \
SET mapreduce.map.java.opts.max.heap=5000; \
SET mapreduce.map.java.opts=-Xmx5000m; \
SET mapreduce.reduce.memory.mb=7000; \
SET mapreduce.reduce.java.opts=-Xmx5000m; \
SET mapreduce.reduce.java.opts.max.heap=5000; \
SET hive.exec.dynamic.partition=true; \
SET hive.exec.dynamic.partition.mode=nonstrict; \
SET hive.exec.max.dynamic.partitions=20000; \
SET hive.exec.max.dynamic.partitions.pernode=20000; \
SET hive.exec.max.created.files = 500000; \
SET hive.merge.mapredfiles=true; \
SET hive.exec.parallel=TRUE; \
SET hive.support.quoted.identifiers=NONE;
END
)

}
compile_generic_hook() {
  compilated_file=$1
  hook=$2
  check=$3
  if [ "$check" != "na" ]; then
    cat $compilated_file | sed "s#$hook#${check}#g" >"${compilated_file}_temp"
    cat "${compilated_file}_temp" > $compilated_file
    rm "${compilated_file}_temp"
  else
    cat $compilated_file | sed "s#$hook#1=1#g" >"${compilated_file}_temp"
    cat "${compilated_file}_temp" > $compilated_file
    rm "${compilated_file}_temp"
  fi
}
compile() {
  dir_compilated=$1
  dir_pilot=$2
  rm -rf "${dir_compilated}"
  mkdir -p "${dir_compilated}"
  log "********************************************"
  for pilot_file in "${dir_pilot}/"*; do
    log "Processing $pilot_file pilot_file..."
    # prendo solo il nome del pilot_file
    compilated_file="${dir_compilated}/comp_${pilot_file##*/}"
    cat ${pilot_file} > ${compilated_file}
    compile_generic_hook $compilated_file "POD-FILTER" "$CHECK_POD"
    compile_generic_hook $compilated_file "PIVA-FILTER" "$CHECK_PIVA_DISTR"
    compile_generic_hook $compilated_file "PIVAUDD-FILTER" "$CHECK_PIVA_UDD"
    compile_generic_hook $compilated_file "ANNOMESE-FILTER" "$CHECK_ANNO_MESE"
    compile_generic_hook $compilated_file "SINGLE-DATA-DECORRENZA-FILTER" "$CHECK_SINGLE_DATA_DECORRENZA"
    compile_generic_hook $compilated_file "PAIRPIVAUDDPIVADIST-FILTER" "$CHECK_PAIR_PIVA_DIST_PIVA_UDD"
    compile_generic_hook $compilated_file "ADDCREATETABLEPURGED-FILTER" "$CHECK_ADDED_VIEW_PURGED"
    compile_generic_hook $compilated_file "VIEWSWCONTRAVIEWRSPURGED-FILTER" "$CHECK_PURGE_VIEW_SW_CON_TRA_VIEW_RS"
    compile_generic_hook $compilated_file "HOLIDAYLIST-FILTER" "$CHECK_HOLIDAYS"
    compile_generic_hook $compilated_file "DATA-FILTER" "$CHECK_DATA"
    compile_generic_hook $compilated_file "MAPREDUCEPARAM-FILTER" "$MAP_REDUCE_PARAM"

    CHECK_POD_RICONF=$(echo $CHECK_POD | sed "s/pod14/pod_config/g")
    compile_generic_hook $compilated_file "PODRICONF-FILTER" "$CHECK_POD_RICONF"

    chmod 777 $compilated_file
  done
  log "********************************************"
}
print_variable() {
  if [[ "${PRINT_VERBOSE}" == "true" ]]; then
    log "FILE_INGESTION_TIMESTAMP: $FILE_INGESTION_TIMESTAMP"
    log "FILE_PODS: $FILE_PODS"
    log "FILE_STORICI_SWITCHING_DATES: $FILE_STORICI_SWITCHING_DATES"
    log "FILE_PIVAS_DISTR: $FILE_PIVAS_DISTR"
    log "FILE_PIVAS_UDD: $FILE_PIVAS_UDD"
    log "FILE_PAIR_PIVAS_UDD_PIVA_DISTR: $FILE_PAIR_PIVAS_UDD_PIVA_DISTR"
  fi
}
print_check() {
  log "********************************************"
  log "CHECK CREATED:"
  log "********************************************"
  log "CHECK_POD: $CHECK_POD"
  log "CHECK_ANNO_MESE: $CHECK_ANNO_MESE"
  log "CHECK_DATA: $CHECK_DATA"
  log "CHECK_PIVA_DISTR: $CHECK_PIVA_DISTR"
  log "CHECK_PIVA_UDD: $CHECK_PIVA_UDD"
  log "CHECK_PAIR_PIVA_DIST_PIVA_UDD: $CHECK_PAIR_PIVA_DIST_PIVA_UDD"
  log "CHECK_ADDED_VIEW_PURGED: $CHECK_ADDED_VIEW_PURGED"
  log "CHECK_PURGE_VIEW_SW_CON_TRA_VIEW_RS: $CHECK_PURGE_VIEW_SW_CON_TRA_VIEW_RS"
  log "********************************************"
  log "MAP_REDUCE_PARAM: $MAP_REDUCE_PARAM"
  log "********************************************"
  log "dir_compilated $dir_compilated"
}
print_hiveconf_var() {
  log $HIVE_VAR
}
convert_var_to_hive_conf_string() {
  HIVE_VAR="-hiveconf SWITCHING_EE_HIVE_DB_NAME=$SWITCHING_EE_HIVE_DB_NAME \
 -hiveconf AU_DB_NAME=$AU_DB_NAME \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME=$SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_TRATT_TABLE_NAME=$SWITCHING_EE_HIVE_STORICI_RS_TRATT_TABLE_NAME \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_MISUR_TABLE_NAME=$SWITCHING_EE_HIVE_STORICI_RS_MISUR_TABLE_NAME \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_STATO_TABLE_NAME=$SWITCHING_EE_HIVE_STORICI_RS_STATO_TABLE_NAME \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME=$SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME \
 -hiveconf SWITCHING_EE_HIVE_STORICI_OUTPUT_TABLE_NAME=$SWITCHING_EE_HIVE_STORICI_OUTPUT_TABLE_NAME \
 -hiveconf SWITCHING_EE_HIVE_FLUSSO_MISURE_QUARTI=$SWITCHING_EE_HIVE_FLUSSO_MISURE_QUARTI \
 -hiveconf SWITCHING_EE_HIVE_FLUSSO_MISURE_ESTENSIONE_QUARTI=$SWITCHING_EE_HIVE_FLUSSO_MISURE_ESTENSIONE_QUARTI \
 -hiveconf SWITCHING_EE_HIVE_FLUSSO_MISURE_NO_AGGR=$SWITCHING_EE_HIVE_FLUSSO_MISURE_NO_AGGR \
 -hiveconf SWITCHING_EE_HIVE_FLUSSO_MISURE_SMIS=$SWITCHING_EE_HIVE_FLUSSO_MISURE_SMIS \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_MISUR_TABLE_NAME_PURGED=${SWITCHING_EE_HIVE_STORICI_RS_MISUR_TABLE_NAME}_purged \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_TRATT_TABLE_NAME_PURGED=${SWITCHING_EE_HIVE_STORICI_RS_TRATT_TABLE_NAME}_purged \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RS_STATO_TABLE_NAME_PURGED=${SWITCHING_EE_HIVE_STORICI_RS_STATO_TABLE_NAME}_purged \
 -hiveconf SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME_PURGED=${SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME}_purged \
 -hiveconf SWITCHING_EE_HIVE_SCARTI_STORICI_OUTPUT_TABLE_NAME=${SWITCHING_EE_HIVE_SCARTI_STORICI_OUTPUT_TABLE_NAME} "

}
run() {
  OPTIND=1
  while getopts d:q: OPT; do
    case "$OPT" in
      d) dir=$OPTARG ;;
      q) QUEUE_OPTION="-hiveconf mapred.job.queue.name=${OPTARG}" ;;
      ??* ) die "Illegal option --$OPT" ;;
      \? ) exit 2 ;;
    esac
  done

  LOADING_TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
  log "LOADING_TIMESTAMP=\"${LOADING_TIMESTAMP}\""

  log "Run: comp_ex_0_create_with_table.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_ex_0_create_with_table.hql" $HIVE_VAR >> "${LOG_FILE}" 2>&1
  CHECK_RESULT=$(beeline \
        -u '${hive.jdbc.url}' \
        -n '${hive.jdbc.user}' \
        --outputformat=csv2 \
        --showHeader=false \
        -e "select * from ${SWITCHING_EE_HIVE_DB_NAME}.bb limit 1;" \
        2>>"${LOG_FILE}")
  if [[ "${CHECK_RESULT}" == "" ]]; then
    die "The current switch combination did not generate data."
  fi

  log "Run: comp_ex_1_create_ingestion_orari.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_ex_1_create_ingestion_orari.hql" $HIVE_VAR $QUEUE_OPTION >> "${LOG_FILE}" 2>&1
  log "Run: comp_ex_1_2_create_sw_misure_no_12m_202002.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_ex_1_2_create_sw_misure_no_12m_202002.hql" $HIVE_VAR $QUEUE_OPTION >> "${LOG_FILE}" 2>&1
  log "Run: comp_create_sw_misure_no_12m_202002_unionall.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_create_sw_misure_no_12m_202002_unionall.hql" $HIVE_VAR >> "${LOG_FILE}" 2>&1
  log "Run: create_storici_xml.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_create_storici_xml.hql" $HIVE_VAR >> "${LOG_FILE}" 2>&1
  log "Run: riconfigurazione_part1.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_riconfigurazione_part1.hql" $HIVE_VAR >> "${LOG_FILE}" 2>&1
  log "Run: riconfigurazione_part2.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_riconfigurazione_part2.hql" $HIVE_VAR >> "${LOG_FILE}" 2>&1

  log "Run: insert_into_dati_storici.sql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}'  -f "${dir}/comp_insert_into_dati_storici.sql" $HIVE_VAR -hiveconf loading_timestamp="$LOADING_TIMESTAMP" >> "${LOG_FILE}" 2>&1
  log "Run: insert_into_dati_storici_riconf.sql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_insert_into_dati_storici_riconf.sql" $HIVE_VAR -hiveconf loading_timestamp="$LOADING_TIMESTAMP" >> "${LOG_FILE}" 2>&1

  log "Run: comp_insert_into_scarti_storici.hql"
  beeline -u '${hive.jdbc.url}' -n '${hive.jdbc.user}' -f "${dir}/comp_insert_into_scarti_storici.hql" $HIVE_VAR -hiveconf loading_timestamp="$LOADING_TIMESTAMP" >> "${LOG_FILE}" 2>&1
}
