select count(r.filename) from (
    select split(lower(filename), '.zip')[0] as filename from  cmg_gas.report_file	
    where filename_src like '%PERIODO%'
    and dataelaborazione='DATA'
) as r
left join cmg_gas.report_decompressione as p
on r.filename = split(reverse(split(reverse(lower(p.filename_src)),'/')[0]), '.zip')[0]
where p.filename_src is null
