select count(distinct filename_src) from cmg_gas.report_decompressione
where filename_src like'%PERIODO%'
