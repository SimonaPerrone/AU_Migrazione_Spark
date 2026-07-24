select count(*) from  cmg_gas.report_file	
where filename_src like '%PERIODO%'
    and dataelaborazione='DATA'
    and filename like '%FLUSSO%'
