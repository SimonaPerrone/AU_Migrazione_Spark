

import os
import zipfile
import datetime
import re
import shutil
from pyspark.sql.types import *

class Job19:
    """
    Job estrazioni dati dal File SAG per i PdR MM e YY (mensile e altro).
    """
    def __init__(self, conf):
        self.conf = conf
        
        self.nameTableHiveDELTA = conf["job19"]["nameTableHiveDELTA"]
        self.nameTableHivePDR = conf["job19"]["nameTableHivePDR"]
        self.cmdTableRefreshDELTA = conf["job19"]["cmdTableRefreshDELTA"]
        self.cmdTableRefreshPDR = conf["job19"]["cmdTableRefreshPDR"]
        self.database = conf["job19"]["database"]
        self.tableDelta = conf["job19"]["tableDelta"]
        self.tablePDR = conf["job19"]["tablePDR"]


    def clear_table(self, sqlCtx):
        sqlCtx.sql("TRUNCATE TABLE " + self.tableDelta)
        sqlCtx.sql("TRUNCATE TABLE " + self.nameTableHivePDR)

    def backup_table(self, sqlCtx):
        date = datetime.datetime.now()
        import subprocess
        date = datetime.datetime.now()
        try:
            cmd = ("hdfs dfs -ls "+ self.nameTableHiveDELTA + " ").split() # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if (len(filter(None,files)) > 0):
                query = "LOAD DATA  INPATH  '" + self.nameTableHiveDELTA + "' OVERWRITE INTO TABLE " + self.tableDelta + "_backup PARTITION (DATA_BACKUP='" + str(date.year) + str(date.month) + str(date.day) + "')"
                print query
                sqlCtx.sql(query)
        except:
            pass

        try:
            cmd = ("hdfs dfs -ls "+ self.nameTableHivePDR + " ").split() # cmd must be an array of arguments
            files = subprocess.check_output(cmd).strip().split('\n')
            if (len(filter(None,files)) > 0):
                query = "LOAD DATA  INPATH  '" + self.nameTableHivePDR + "' OVERWRITE INTO TABLE " + self.tablePDR + "_backup PARTITION (DATA_BACKUP='" + str(date.year) + str(date.month) + str(date.day) + "')"
                print query
                sqlCtx.sql(query)
        except:
            pass

    def run(self, sc, sqlCtx, params):
        """
        Si richiede un job che a partire dai dati del SAG caricati al punto 8 costruisca le tabelle:
        -	 TAB_PDR_SEGMENTO_DELTA_MIS_MM cosi definita: 
             PDR - DATA_Dz - DATA_ Dz+1 – Delta_Mis – DATA_Az - DATA_Az+1 - TRATTAMENTO

        -	TAB_PDR_SEGMENTO_PDR_MIS_MM così definita :
            PDR - DATA_Dz - DATA_ Dz+1 – ?Prof,Nk,DZ -  ?Prof, Nk, Az 

        se TRATTAMENTO sono 'M' O 'Y' e la differenza tra DATA_Dz - DATA_Dz+1 è maggiore o uguale a 300 giorni:
        -	copia i valori PDR, DATA_Dz e DATA_Dz+1  in entrambe le tabelle
        -	copia i valori Delta_Mis  dal tracciato SAG.
        -	Imposta in TAB_PDR_SEGMENTO_DELTA_MIS_MM 
                i campi DATA_Az - DATA_Az+1 rispettivamente con DATA_DZ - DATA_DZ+1
        -	Imposta in TAB_PDR_SEGMENTO_PDR_MIS_MM 
                i campi ?Prof,Nk,DZ -  ?Prof, Nk,Az  sempre a 1.

        """

        self.backup_table(sqlCtx)
        #self.clear_table(sqlCtx)

        query1 = "select distinct                                                                                                                                                                                                                                                       "\
        + "    gas_sag.cod_pdr as PDR,                                                                                                                                                                                                                                                  "\
        + "    gas_sag.data_mis1 as DATA_DZ,                                                                                                                                                                                              "\
        + "    gas_sag.data_mis2 as DATA_DZ1,                                                                                                                                                                                             "\
        + "    datediff(gas_sag.data_mis2, gas_sag.data_mis1) as diff,                                                                                                                      "\
        + "    (                                                                                                                                                                                                                                                                        "\
        + "        case                                                                                                                                                                                                                                                                 "\
        + "            when (rcugas_massivo.t_trattamento = 'Y' or rcugas_massivo.t_trattamento = 'M') and datediff(gas_sag.data_mis2, gas_sag.data_mis1) >= 300 then gas_sag.cons_ann      "\
        + "        end                                                                                                                                                                                                                                                                  "\
        + "    )as DELTA_MIS,                                                                                                                                                                                                                                                           "\
        + "    (                                                                                                                                                                                                                                                                        "\
        + "        case                                                                                                                                                                                                                                                                 "\
        + "            when (rcugas_massivo.t_trattamento = 'Y' or rcugas_massivo.t_trattamento = 'M') and datediff(gas_sag.data_mis2,gas_sag.data_mis1) >= 300 then gas_sag.data_mis1     "\
        + "        end                                                                                                                                                                                                                                                                  "\
        + "    ) as DATA_Az,                                                                                                                                                                                                                                                            "\
        + "    (                                                                                                                                                                                                                                                                        "\
        + "        case                                                                                                                                                                                                                                                                 "\
        + "            when (rcugas_massivo.t_trattamento = 'Y' or rcugas_massivo.t_trattamento = 'M') and datediff(gas_sag.data_mis2,gas_sag.data_mis1) >= 300 then gas_sag.data_mis2     "\
        + "        end                                                                                                                                                                                                                                                                  "\
        + "    ) as DATA_Az1,                                                                                                                                                                                                                                                           "\
        + "    rcugas_massivo.t_trattamento as TRATTAMENTO                                                                                                                                                                                                                              "\
        + "    from " + self.database + ".gas_sag                                                                                                                                                                                                                                       "\
        + "    inner join rcugas.rcugas_massivo on rcugas_massivo.t_codice_pdr = gas_sag.cod_pdr                                                                                                                                                                                        "\
        + "    where rcugas_massivo.t_trattamento is not null and rcugas_massivo.t_trattamento <> 'null'                                                                                                                                                                                "


        query2 = "select distinct                                                                                                                                                                                                                                                           "\
        + "    gas_sag.cod_pdr as PDR,                                                                                                                                                                                                                                                      "\
        + "    gas_sag.data_mis1 as DATA_DZ,                                                                                                                                                                                                  "\
        + "    gas_sag.data_mis2 as DATA_DZ1,                                                                                                                                                                                                 "\
        + "    datediff(gas_sag.data_mis2,gas_sag.data_mis1) as diff,                                                                                                                          "\
        + "    (                                                                                                                                                                                                                                                                            "\
        + "        case                                                                                                                                                                                                                                                                     "\
        + "            when (rcugas_massivo.t_trattamento = 'Y' or rcugas_massivo.t_trattamento = 'M') and datediff(gas_sag.data_mis2, gas_sag.data_mis1) >= 300 then cast(1.0 as double)       "\
        + "        end                                                                                                                                                                                                                                                                      "\
        + "    ) as PProfNkDZ,                                                                                                                                                                                                                                                              "\
        + "    (                                                                                                                                                                                                                                                                            "\
        + "        case                                                                                                                                                                                                                                                                     "\
        + "            when (rcugas_massivo.t_trattamento = 'Y' or rcugas_massivo.t_trattamento = 'M') and datediff(gas_sag.data_mis2 , gas_sag.data_mis1 ) >= 300 then cast(1.0 as double)       "\
        + "        end                                                                                                                                                                                                                                                                      "\
        + "    ) as PProfNkAz                                                                                                                                                                                                                                                               "\
        + "    from " + self.database + ".gas_sag                                                                                                                                                                                                                                           "\
        + "    inner join rcugas.rcugas_massivo on rcugas_massivo.t_codice_pdr = gas_sag.cod_pdr                                                                                                                                                                                            "\
        + "    where rcugas_massivo.t_trattamento is not null and rcugas_massivo.t_trattamento <> 'null'                                                                                                                                                                                    "


        TAB_PDR_SEGMENTO_DELTA_MIS_MM = sqlCtx.sql(query1)
        TAB_PDR_SEGMENTO_PDR_MIS_MM  = sqlCtx.sql(query2)

        #TAB_PDR_SEGMENTO_DELTA_MIS_MM.show()
        #TAB_PDR_SEGMENTO_PDR_MIS_MM.show()

        TAB_PDR_SEGMENTO_DELTA_MIS_MM.write.parquet(self.nameTableHiveDELTA, 'append')
        TAB_PDR_SEGMENTO_PDR_MIS_MM.write.parquet(self.nameTableHivePDR, 'append')
 
        # print self.nameTableHiveDELTA
        # print self.nameTableHivePDR
        # 
        sqlCtx.sql(self.cmdTableRefreshDELTA)
        sqlCtx.sql(self.cmdTableRefreshPDR)

        pass
