import xml.etree.ElementTree as ET
import constants

import hashlib
import datetime
import os
import re

from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import *

class FlussoTGL:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = ""
        # Flusso TGL
        if codice_servizio == constants.CODSERVIZIO_TGL and codice_flusso == "0050":
            file_xsd = constants.FILE_XSD_TGL_0050
        return file_xsd

    def isValidFile(self, filename):
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(T|t)(G|g)(L|l)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []   

        mese_comp, cod_pdr, matr_mis, matr_conv, val_dato_mens, esito_raccolta, \
        data_comp, let_tot_prel, let_tot_conv, tipo_lettura = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, mese_comp, cod_pdr, matr_mis, val_dato_mens, esito_raccolta,
             data_comp, let_tot_prel, tipo_lettura, anno, mese, local_file)
        )

        return result

    def write(self, rdd, pool_id, sc, sqlCtx):
        print("Thread TGL")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfTGL = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoTGL)
        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        annomese_rif_udf = udf(
            lambda d: str(os.path.basename(d).split("_")[2][0:4] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6] if (len(os.path.basename(d).split("_")[2]) > 4) else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        dfTGL = dfTGL.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfTGL = dfTGL.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfTGL = dfTGL.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfTGL = dfTGL.withColumn("d_caricamento",  lit(dataElaborazione))
        dfTGL = dfTGL.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        #constants.PATHDFS_TGL = "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tgl_p"
        constants.PARTITIONLIST_TGL =["mese_comp"]
        print("Write: ", constants.PATHDFS_TGL)
        print("Partition List: ", constants.PARTITIONLIST_TGL)
        dfTGL.write.partitionBy(constants.PARTITIONLIST_TGL).parquet(constants.PATHDFS_TGL, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_TGL)

        #sqlCtx.sql("TRUNCATE TABLE cmg_gas.prt_cmg_tgl_p_tmp")
        #constants.PATHDFS_TGL = "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tgl_p_tmp"
        #print("Write: ", constants.PATHDFS_TGL)
        #mode='overwrite' # mode='append'
        #dfTGL.write.parquet(constants.PATHDFS_TGL, mode)

        #constants.CMD_REFRESH_TGL="MSCK REPAIR TABLE cmg_gas.prt_cmg_tgl_p_tmp"
        #sqlCtx.sql(constants.CMD_REFRESH_TGL)

        #dfTGL_finale = sqlCtx.read.parquet("/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tgl_p_tmp")
        
        #constants.PATHDFS_TGL ="/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tgl_p_v2"
        #print("Write: ", constants.PATHDFS_TGL)
        #dfTGL_finale.coalesce(1).write.partitionBy(["mese_comp"]).parquet(constants.PATHDFS_TGL, 'append')

        #constants.CMD_REFRESH_TGL="MSCK REPAIR TABLE cmg_gas.prt_cmg_tgl_p_v2"
        #sqlCtx.sql(constants.CMD_REFRESH_TGL)


        sc.setLocalProperty("spark.scheduler.pool", None)

    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        result = []

        if root is None:
            return self.makeError(dataElaborazione, anno, mese, local_file)

        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso = None if root.get("cod_flusso") is None else root.get("cod_flusso")

        piva_utente = None \
            if root.find("IdentificativiRichiesta/piva_utente") is None \
            else root.find("IdentificativiRichiesta/piva_utente").text

        piva_distr = None \
            if root.find("IdentificativiRichiesta/piva_distr") is None \
            else root.find("IdentificativiRichiesta/piva_distr").text

        mese_comp = "EE" \
            if root.find("IdentificativiRichiesta/mese_comp") is None \
            else root.find("IdentificativiRichiesta/mese_comp").text

        if anno == "" or mese == "" or piva_distr == "" or piva_utente == "":
            return self.makeError(dataElaborazione, "EE","EE", local_file)

        if mese_comp is not None:
            mese_comp = mese_comp.replace("/", "")

        DatiPdR = root.findall("DatiPdR")

        if DatiPdR is None:
            print("Warn", "flusso tgl non valido")
            return self.makeError(dataElaborazione, anno, mese, local_file)

        for item in DatiPdR:
            cod_pdr = None if item.find("cod_pdr") is None else item.find("cod_pdr").text
            matr_mis = None if item.find("matr_mis") is None else item.find("matr_mis").text
            matr_conv = None if item.find("matr_conv") is None else item.find("matr_conv").text
            val_dato_mens = None if item.find("val_dato_mens") is None else item.find("val_dato_mens").text
            esito_raccolta = None if item.find("esito_raccolta") is None else item.find("esito_raccolta").text

            Letture = item.findall("Lettura")

            if Letture != None:
                for lettura in Letture:
                    data_comp = None if lettura.find("data_comp") is None else lettura.find("data_comp").text
                    let_tot_prel = None if lettura.find("let_tot_prel") is None else lettura.find("let_tot_prel").text
                    let_tot_conv = None if lettura.find("let_tot_conv") is None else lettura.find("let_tot_conv").text
                    tipo_lettura = None if lettura.find("tipo_lettura") is None else lettura.find("tipo_lettura").text

                    result.append(
                        (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr, mese_comp,
                         cod_pdr, matr_mis, matr_conv, val_dato_mens, esito_raccolta, data_comp, let_tot_prel,
                         let_tot_conv, tipo_lettura, anno, mese, local_file)
                    )
        
        return result

    def reset(self):
        mese_comp = "EE"
        cod_pdr = None
        matr_mis = None
        matr_conv = None

        val_dato_mens = None
        esito_raccolta = None
        data_comp = None

        let_tot_prel = None
        let_tot_conv = None
        tipo_lettura = None

        return mese_comp, cod_pdr, matr_mis, matr_conv, val_dato_mens, esito_raccolta, \
               data_comp, let_tot_prel, let_tot_conv, tipo_lettura
