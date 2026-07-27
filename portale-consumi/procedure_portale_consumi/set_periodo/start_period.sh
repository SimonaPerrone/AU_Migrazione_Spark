#!/bin/bash

limit_date=$1


if [[ (! -v limit_date) || -z "$limit_date" ]];then
 limit_date='2019-01-01'
fi

if [ "z$limit_date" != "z" ] && date -d "$limit_date" >/dev/null
then
  echo "data $limit_date valida!"
else
  limit_date='2019-01-01'
  echo "applicata data $limit_date"
fi

export HIVE_SKIP_SPARK_ASSEMBLY=true;
export limit_date

periodo=$(hive -f /mnt/isilonshare1/Software/EE/portale_consumi/set_periodo/get_days.hql)
echo "giorni : $periodo"

unset limit_date
unset HIVE_SKIP_SPARK_ASSEMBLY

rm -f /mnt/isilonshare1/Software/EE/portale_consumi/set_periodo/.periodo
echo $periodo > /mnt/isilonshare1/Software/EE/portale_consumi/set_periodo/.periodo

