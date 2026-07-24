select sum(c) as tot
from (
       select filename_folder_dest, count(filename_folder_dest) as c
       from cmg_gas.report_decompressione
       where cast(dataelaborazione as date) = "DATA"
             and lower(filename_src) not like "%.zip"
       group by filename_folder_dest
       having c > 1
) as T;
