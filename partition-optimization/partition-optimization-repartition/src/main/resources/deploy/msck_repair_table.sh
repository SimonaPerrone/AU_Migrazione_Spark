param_properties_path="${path.properties}/params.properties"

LIST_TABLE=$(${is.local} $param_properties_path | grep 'tables.to.optimize' |  sed 's/tables.to.optimize =//')

for table in $(echo $LIST_TABLE | tr "," "\n")
do
  hive -u jdbc:hive2://dmphclo17.siiau.local:10000 -n ${system.user} -e "msck repair table $table"
done

