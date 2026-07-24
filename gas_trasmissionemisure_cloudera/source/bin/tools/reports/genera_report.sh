ANNO=`date -d "$DD" +%Y`
MESE=`date -d "$DD" +%m`
GIORNO=`date -d "$DD" +%d`
BASEPATH_LOG="/home/silvia"
CURDD=`date +%Y-%m-%d`

GIORNO_ELAB=`date -d "$DD" +%d`
DD=`date +%Y-%m-%d -d "$CURDD -1 day"`
CURTMS=`date +%Y%m%d%H%M%S`
GIORNO=`date -d "$DD" +%d`

#ANNO="2020"
#MESE="04"
#GIORNO="02"
#GIORNO_ELAB="03"
PERIODO="$ANNO/$MESE$GIORNO"
DATA="$ANNO-$MESE-$GIORNO_ELAB"
#echo "query: "
#cat sql/distinct_count.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g"

count=0
count_items=20


progress_bar () {
   item_completed=$1
   items=$2
   mesg=$3

   item_percent=$(((item_completed*100)/$items))
   echo -en "COMPLETO al: ${item_percent}% ${mesg}\0015"
}

progress_bar $count $count_items "Query1 (sql/distinct_count.sql)                                                                                                            "
count=$((count+1))
NUMBER_DISTINCT_TXT=$(hive -e "$(cat sql/distinct_count.sql | sed "s|PERIODO|${PERIODO}|" | sed "s|DATA|${DATA}|g")" 2> /dev/null | grep -v WARN | grep -v INFO) 

#echo $NUMBER_DISTINCT_TXT
mkdir -p reports 2> /dev/null
report="reports/report_$DATA.md"
cp report_template.md $report
sed -i "s|PERIODO|${PERIODO}|g" $report
sed -i "s|DATA|${DATA}|g" $report 
sed -i "s|NUMBER_DISTINCT_TXT|${NUMBER_DISTINCT_TXT}|g" $report

#RGL counts
#cat sql/count_rgl.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|RGL|g"

NUMBER_RGL=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|RGL|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql RGL)                                                                                                          "
count=$((count+1))

NUMBER_RML=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|RML|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql RML)                                                                                                          "
count=$((count+1))

NUMBER_RSL=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|RSL|g")" 2> /dev/null | grep -v WARN  | grep -v INFO)
progress_bar $count $count_items "Query2 (sql/count_flusso.sql RSL)                                                                                                          "
count=$((count+1))

NUMBER_RMV=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|RMV|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql RMV)                                                                                                          "
count=$((count+1))

NUMBER_SW1=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|SW1|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql SW1)                                                                                                          "
count=$((count+1))

NUMBER_TAL=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|TAL|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql TAL)                                                                                                          "
count=$((count+1))

NUMBER_TAS=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|TAS|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql TAS)                                                                                                          "
count=$((count+1))

NUMBER_TAV=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|TAV|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql TAV)                                                                                                          "
count=$((count+1))

NUMBER_TGL=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|TGL|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql TGL)                                                                                                          "
count=$((count+1))

NUMBER_TML=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|TML|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql TML)                                                                                                          "
count=$((count+1))

NUMBER_TMV=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|TMV|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql TMV)                                                                                                          "
count=$((count+1))

NUMBER_DEF=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|DEF|g")" 2> /dev/null | grep -v WARN  | grep -v INFO)
progress_bar $count $count_items "Query2 (sql/count_flusso.sql DEF)                                                                                                          "
count=$((count+1))

NUMBER_FUI=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|FUI|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql FUI)                                                                                                          "
count=$((count+1))

NUMBER_IM1=$(hive -e "$(cat sql/count_flusso.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" | sed "s|FLUSSO|IM1|g")" 2> /dev/null | grep -v WARN  | grep -v INFO) 
progress_bar $count $count_items "Query2 (sql/count_flusso.sql IM1)                                                                                                          "
count=$((count+1))

wait

#echo "Numero RGL $NUMBER_RGL"
sed -i "s|NUMBER_RGL|${NUMBER_RGL}|g" -i $report
sed -i "s|NUMBER_RML|${NUMBER_RML}|g" -i $report
sed -i "s|NUMBER_RSL|${NUMBER_RSL}|g" -i $report
sed -i "s|NUMBER_RMV|${NUMBER_RMV}|g" -i $report
sed -i "s|NUMBER_SW1|${NUMBER_SW1}|g" -i $report
sed -i "s|NUMBER_TAL|${NUMBER_TAL}|g" -i $report
sed -i "s|NUMBER_TAS|${NUMBER_TAS}|g" -i $report
sed -i "s|NUMBER_TAV|${NUMBER_TAV}|g" -i $report
sed -i "s|NUMBER_TGL|${NUMBER_TGL}|g" -i $report
sed -i "s|NUMBER_TML|${NUMBER_TML}|g" -i $report
sed -i "s|NUMBER_TMV|${NUMBER_TMV}|g" -i $report
sed -i "s|NUMBER_DEF|${NUMBER_DEF}|g" -i $report
sed -i "s|NUMBER_FUI|${NUMBER_FUI}|g" -i $report
sed -i "s|NUMBER_IM1|${NUMBER_IM1}|g" -i $report

#
#cat sql/count_report_decompressi.sql |sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" 
NUMBER_REPORT_DECOMPRESSI=$(hive -e "$(cat sql/count_report_decompressi.sql |sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g" )" 2> /dev/null | grep -v WARN | grep -v INFO) 
#echo "NUMBER_REPORT_DECOMPRESSI: $NUMBER_REPORT_DECOMPRESSI"
sed -i "s|NUMBER_REPORT_DECOMPRESSI|${NUMBER_REPORT_DECOMPRESSI}|g" -i $report
progress_bar $count $count_items "Query3 (sql/count_report_decompressi.sql)                                                                                                 "
count=$((count+1))

#cat sql/count_report_decompressi_2.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g"
NUMBER_REPORT_2_DECOMPRESSI=$(hive -e "$(cat sql/count_report_decompressi_2.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g")" 2> /dev/null | grep -v WARN | grep -v INFO) 
#echo "NUMBER_REPORT_2_DECOMPRESSI: $NUMBER_REPORT_2_DECOMPRESSI"
sed -i "s|NUMBER_REPORT_2_DECOMPRESSI|${NUMBER_REPORT_2_DECOMPRESSI}|g" -i $report
progress_bar $count $count_items "Query4 (sql/count_report_decompressi_2.sql)                                                                                               "
count=$((count+1))

#cat sql/zeppelin_report_1.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g"
NUMBER_ZEPPELIN_1_REPORT_DECOMPRESSI=$(hive -e "$(cat sql/zeppelin_report_1.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g")" 2> /dev/null | grep -v WARN | grep -v INFO) 
#echo "NUMBER_ZEPPELIN_REPORT_1_DECOMPRESSI: $NUMBER_ZEPPELIN_1_REPORT_DECOMPRESSI"
sed -i "s|NUMBER_ZEPPELIN_1_REPORT_DECOMPRESSI|${NUMBER_ZEPPELIN_1_REPORT_DECOMPRESSI}|g" -i $report
progress_bar $count $count_items "Query5 (sql/zeppelin_report_1.sql)                                                                                                        "
count=$((count+1))

#cat sql/zeppelin_report_2.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g"
NUMBER_ZEPPELIN_REPORT_2=$(hive -e "$(cat sql/zeppelin_report_2.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g")" 2> /dev/null | grep -v WARN | grep -v INFO) 
#echo "NUMBER_ZEPPELIN_REPORT_2: $NUMBER_ZEPPELIN_REPORT_2"
sed -i "s|NUMBER_ZEPPELIN_REPORT_2|${NUMBER_ZEPPELIN_REPORT_2}|g" -i $report
progress_bar $count $count_items "Query6 (sql/zeppelin_report_2.sql)                                                                                                        "
count=$((count+1))

NUMBER_ZEPPELIN_3_REPORT=$(hive -e "$(cat sql/zeppelin_report_3.sql | sed "s|PERIODO|${PERIODO}|g" | sed "s|DATA|${DATA}|g")" 2> /dev/null | grep -v WARN | grep -v INFO)
#echo "NUMBER_ZEPPELIN_3_REPORT: $NUMBER_ZEPPELIN_3_REPORT"
sed -i "s|NUMBER_ZEPPELIN_3_REPORT|${NUMBER_ZEPPELIN_3_REPORT}|g" -i $report
progress_bar $count $count_items "Query6 (sql/zeppelin_report_2.sql)                                                                                                        "
count=$((count+1))



wait
