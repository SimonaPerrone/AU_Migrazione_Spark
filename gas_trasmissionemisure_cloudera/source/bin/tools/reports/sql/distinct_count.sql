select count(distinct split(lower(filename),'.zip')[0]) from cmg_gas.report_file	
where filename_src like '%PERIODO%'
--    and dataelaborazione='DATA';
