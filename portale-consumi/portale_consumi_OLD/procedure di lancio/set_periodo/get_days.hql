set hive.cli.print.header=false;

select case when datediff(current_date,'${env:limit_date}')  > 1126 then 1126 else datediff(current_date,'${env:limit_date}') end as dd; 

