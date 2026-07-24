import constants
import hashlib
import datetime
import os
import re

from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import *

class FlussoTAS:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        pattern = "^\\d{11,16}_\\d{11,16}_\\d{6}_(T|t)(A|a)(S|s)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []
        DatiPdr_cod_pdr, DatiPdr_matr_mis, DatiPdr_matr_conv, DatiPdr_data_com_autolet_cf, \
        DatiPdr_let_tot_prel, DatiPdr_let_tot_conv, DatiPdr_esito_val, DatiPdr_note = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, DatiPdr_cod_pdr,
             DatiPdr_matr_mis, DatiPdr_matr_conv, DatiPdr_data_com_autolet_cf, DatiPdr_let_tot_prel,
             DatiPdr_let_tot_conv, DatiPdr_esito_val, DatiPdr_note, anno, mese, local_file)
        )
        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = ""
        # Flusso TAS
        if codice_servizio == constants.CODSERVIZIO_TAS and codice_flusso == "0050":
            file_xsd = constants.FILE_XSD_TAS_0050
        elif codice_servizio == constants.CODSERVIZIO_TAS and codice_flusso == "0150":
            file_xsd = constants.FILE_XSD_TAS_0150
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print("Thread TAS")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))   
        dfTAS = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoTAS)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        # data_com_autolet_cf gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        annomese_rif_udf = udf(
            lambda d: str(
                os.path.basename(d).split("_")[2][0:4]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else os.path.basename(d).split("_")[2][2:4]) + str(os.path.basename(d).split("_")[2][4:6]
                         if (len(os.path.basename(d).split("_")[2]) > 4)
                         else os.path.basename(d).split("_")[2][0:2])
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        dfTAS = dfTAS.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfTAS = dfTAS.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfTAS = dfTAS.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        dfTAS = dfTAS.withColumn("annomese", annomese_udf(col('data_com_autolet_cf')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfTAS = dfTAS.withColumn("d_caricamento",  lit(dataElaborazione))
        dfTAS = dfTAS.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))
        #dfTAS = dfTAS.withColumn("data_com_autolet_cf", expr("from_unixtime(unix_timestamp(data_com_autolet_cf, 'dd/MM/yyyy'))").cast("timestamp"))

	#constants.PATHDFS_TAS="/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tas_p_test"
        print("Partition List: ", constants.PARTITIONLIST_TAS)
        print("Write: ", constants.PATHDFS_TAS)
        dfTAS.write.partitionBy(constants.PARTITIONLIST_TAS).parquet(constants.PATHDFS_TAS, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_TAS)
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

        if anno == "" or mese == "" or piva_distr == "" or piva_utente == "":
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        DatiPdR = root.findall("DatiPdR")

        if DatiPdR is None:
            print ("Warn", "flusso TAS non valido")
            return self.makeError(dataElaborazione, anno, mese, local_file)

        for item in DatiPdR:
            DatiPdr_cod_pdr = None if item.find("cod_pdr") is None else item.find("cod_pdr").text
            DatiPdr_matr_mis = None if item.find("matr_mis") is None else item.find("matr_mis").text
            DatiPdr_matr_conv = None if item.find("matr_conv") is None else item.find("matr_conv").text
            DatiPdr_data_com_autolet_cf = None if item.find("data_com_autolet_cf") is None else item.find("data_com_autolet_cf").text
            DatiPdr_let_tot_prel = None if item.find("let_tot_prel") is None else item.find("let_tot_prel").text
            DatiPdr_let_tot_conv = None if item.find("let_tot_conv") is None else item.find("let_tot_conv").text
            DatiPdr_esito_val = None if item.find("esito_val") is None else item.find("esito_val").text
            DatiPdr_note = None if item.find("note") is None else constants.get_text(item.find("note").text)

            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr, DatiPdr_cod_pdr,
                 DatiPdr_matr_mis, DatiPdr_matr_conv, DatiPdr_data_com_autolet_cf, DatiPdr_let_tot_prel,
                 DatiPdr_let_tot_conv, DatiPdr_esito_val, DatiPdr_note, anno, mese, local_file)
            )
            
        return result

    def reset(self):
        DatiPdr_cod_pdr = None
        DatiPdr_matr_mis = None
        DatiPdr_matr_conv = None
        DatiPdr_data_com_autolet_cf = None
        DatiPdr_let_tot_prel = None
        DatiPdr_let_tot_conv = None
        DatiPdr_esito_val = None
        DatiPdr_note = None

        return DatiPdr_cod_pdr, DatiPdr_matr_mis, DatiPdr_matr_conv, DatiPdr_data_com_autolet_cf, \
               DatiPdr_let_tot_prel, DatiPdr_let_tot_conv, DatiPdr_esito_val, DatiPdr_note
