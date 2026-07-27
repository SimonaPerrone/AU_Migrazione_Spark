CONFIG_PATH="/user/eng_test/agg/deploy/params.properties"

hdfs dfs -cat $CONFIG_PATH | grep -E .*\.basepath= | sed -e 's/.*\.basepath=//g'| sed $'s/\r//'| while read -r line ; do 
	HDFS_PATH="$line"
	if hdfs dfs -test -e $HDFS_PATH; 
	then
		echo "[OK] [$HDFS_PATH]"
	else 
		echo "[ERROR] [$HDFS_PATH]"
		exit 1
    
	fi

done