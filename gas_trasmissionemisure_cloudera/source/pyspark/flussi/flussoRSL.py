import constants
import hashlib
import datetime
import os
import re

from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import *

class FlussoRSL:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(R|r)(S|s)(L|l)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []
        cod_pdr, matr_mis, matr_conv, coeff_corr, progr_anno_term, data_comp, let_tot_prel, let_tot_conv, \
        mot_rett_lett = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, cod_pdr, matr_mis, matr_conv, coeff_corr,
             progr_anno_term, data_comp, let_tot_prel, let_tot_conv, mot_rett_lett, anno, mese, local_file)
        )
        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = ""
        # Flusso RSL
        if codice_servizio == constants.CODSERVIZIO_RSL and codice_flusso == "0400":
            file_xsd = constants.FILE_XSD_RSL_0400
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print("Thread RSL")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfRSL = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoRSL)
        #dfRSL = dfRSL.withColumn("data_comp", expr("from_unixtime(unix_timestamp(data_comp, 'dd/MM/yyyy'))").cast("timestamp"))

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        annomese_rif_udf = udf(
            lambda d: str(os.path.basename(d).split("_")[2][0:4]
                          if (len(os.path.basename(d).split("_")[2]) > 4)
                          else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6]
                                   if (len(os.path.basename(d).split("_")[2]) > 4)
                                   else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        # data_comp gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        dfRSL = dfRSL.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfRSL = dfRSL.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfRSL = dfRSL.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        dfRSL = dfRSL.withColumn("annomese", annomese_udf(col('data_comp')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfRSL = dfRSL.withColumn("d_caricamento",  lit(dataElaborazione))
        dfRSL = dfRSL.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))
       
 
        print ("Write: ", constants.PATHDFS_RSL)
        dfRSL.write.partitionBy(constants.PARTITIONLIST_RSL).parquet(constants.PATHDFS_RSL, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_RSL)
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

        if anno == "" or mese == "" or piva_distr == "" or piva_utente =="":
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        DatiPdR = root.findall("DatiPdR")

        if DatiPdR is None:
            print("Warn", "flusso RSL non valido")
            return self.makeError(dataElaborazione, anno, mese, local_file)

        for item in DatiPdR:
            cod_pdr = None if item.find("cod_pdr") is None else item.find("cod_pdr").text
            matr_mis = None if item.find("matr_mis") is None else item.find("matr_mis").text
            matr_conv = None if item.find("matr_conv") is None else item.find("matr_conv").text
            coeff_corr = None if item.find("coeff_corr") is None else item.find("coeff_corr").text
            progr_anno_term = None if item.find("progr_anno_term") is None else item.find("progr_anno_term").text
            data_comp = "EE" if item.find("data_comp") is None else item.find("data_comp").text
            let_tot_prel = None if item.find("let_tot_prel") is None else item.find("let_tot_prel").text
            let_tot_conv = None if item.find("let_tot_conv") is None else item.find("let_tot_conv").text
            mot_rett_lett = None if item.find("mot_rett_lett") is None else item.find("mot_rett_lett").text

	    if data_comp == "":
		data_comp = "EE"

            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr, cod_pdr, matr_mis, matr_conv,
                 coeff_corr, progr_anno_term, data_comp, let_tot_prel, let_tot_conv, mot_rett_lett, anno, mese,
                 local_file)
            )
            
        return result

    def reset(self):
        """
        Reset
        :return: Resetta gli elementi
        """
        cod_pdr = None
        matr_mis = None
        matr_conv = None
        coeff_corr = None
        progr_anno_term = None
        data_comp = None
        let_tot_prel = None
        let_tot_conv = None
        mot_rett_lett = None

        return cod_pdr, matr_mis, matr_conv, coeff_corr, progr_anno_term, data_comp, let_tot_prel, let_tot_conv, \
               mot_rett_lett

