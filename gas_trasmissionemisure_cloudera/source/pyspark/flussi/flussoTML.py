import constants
import hashlib
import datetime
import os
import re

from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import *

class FlussoTML:

    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(T|t)(M|m)(L|l)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        result = []

        cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, acc_mis, data_racc, let_tot_prel, let_tot_conv, \
        tipo_lettura, val_dato, num_tentativi, esito_raccolta, causa_manc_raccolta, mod_alt_racc, \
        dir_indennizzo, pros_fin = self.reset()

        result.append(
            (None, None, dataElaborazione, None, None, cod_pdr, matr_mis, matr_conv,
             coeff_corr, freq_let, acc_mis, data_racc, let_tot_prel, let_tot_conv, tipo_lettura, val_dato,
             num_tentativi, esito_raccolta, causa_manc_raccolta, mod_alt_racc, dir_indennizzo, pros_fin,
             anno, mese, local_file)
        )
        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        file_xsd = ""
        # Flusso TML
        if codice_servizio == constants.CODSERVIZIO_TML and codice_flusso == "0050":
            file_xsd = constants.FILE_XSD_TML_0050
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        print("Thread TML")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfTML = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoTML)

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        annomese_rif_udf = udf(
            lambda d: str(
                os.path.basename(d).split("_")[2][0:4]
                if (len(os.path.basename(d).split("_")[2]) > 4)
                else os.path.basename(d).split("_")[2][2:4]) + str(
                    os.path.basename(d).split("_")[2][4:6]
                    if (len(os.path.basename(d).split("_")[2]) > 4)
                    else os.path.basename(d).split("_")[2][0:2]
                )
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        #data_racc gg/mm/aaaa
        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        dfTML = dfTML.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfTML = dfTML.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfTML = dfTML.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfTML = dfTML.withColumn("d_caricamento",  lit(dataElaborazione))
        #dfTML = dfTML.withColumn("data_racc", expr("from_unixtime(unix_timestamp(data_racc, 'dd/MM/yyyy'))").cast("timestamp"))
        dfTML = dfTML.withColumn("annomese", annomese_udf(col('data_racc')))
        dfTML = dfTML.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        #constants.PATHDFS_TML = "/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_tml_p"
        print("Partition List: ", constants.PARTITIONLIST_TML)
        print("Write: ", constants.PATHDFS_TML)
        dfTML.write.partitionBy(constants.PARTITIONLIST_TML).parquet(constants.PATHDFS_TML, 'append')
        # dfTML.write.parquet(constants.PATHDFS_TML, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_TML)
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

        if anno == "" or mese == "" or piva_distr == "" or piva_distr == "":
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        DatiPdR = root.findall("DatiPdR")

        if DatiPdR is None:
            print("Warn", "flusso TML non valido")
            return self.makeError(dataElaborazione, "EE", "EE", local_file)

        for item in DatiPdR:
            cod_pdr = None if item.find("cod_pdr") is None else item.find("cod_pdr").text
            matr_mis = None if item.find("matr_mis") is None else item.find("matr_mis").text
            matr_conv = None if item.find("matr_conv") is None else item.find("matr_conv").text
            coeff_corr = None if item.find("coeff_corr") is None else item.find("coeff_corr").text
            freq_let = None if item.find("freq_let") is None else item.find("freq_let").text
            acc_mis = None if item.find("acc_mis") is None else item.find("acc_mis").text
            data_racc = None if item.find("data_racc") is None else item.find("data_racc").text
            let_tot_prel = None if item.find("let_tot_prel") is None else item.find("let_tot_prel").text
            let_tot_conv = None if item.find("let_tot_conv") is None else item.find("let_tot_conv").text
            tipo_lettura = None if item.find("tipo_lettura") is None else item.find("tipo_lettura").text
            val_dato = None if item.find("val_dato") is None else item.find("val_dato").text
            num_tentativi = None if item.find("num_tentativi") is None else item.find("num_tentativi").text
            esito_raccolta = None if item.find("esito_raccolta") is None else item.find("esito_raccolta").text
            causa_manc_raccolta = None if item.find("causa_manc_raccolta") is None else item.find("causa_manc_raccolta").text
            mod_alt_racc = None if item.find("mod_alt_racc") is None else item.find("mod_alt_racc").text
            dir_indennizzo = None if item.find("dir_indennizzo") is None else item.find("dir_indennizzo").text
            pros_fin = None if item.find("pros_fin") is None else item.find("pros_fin").text

            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr, cod_pdr, matr_mis, matr_conv,
                 coeff_corr, freq_let, acc_mis, data_racc, let_tot_prel, let_tot_conv, tipo_lettura, val_dato,
                 num_tentativi, esito_raccolta, causa_manc_raccolta, mod_alt_racc, dir_indennizzo, pros_fin,
                 anno, mese, local_file)
            )

        return result

    def reset(self):
        cod_pdr = None
        matr_mis = None
        matr_conv = None
        coeff_corr = None
        freq_let = None
        acc_mis = None
        data_racc = None
        let_tot_prel = None
        let_tot_conv = None
        tipo_lettura = None
        val_dato = None
        num_tentativi = None
        esito_raccolta = None
        causa_manc_raccolta = None
        mod_alt_racc = None
        dir_indennizzo = None
        pros_fin = None

        return cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, acc_mis, data_racc, let_tot_prel, let_tot_conv,\
               tipo_lettura, val_dato, num_tentativi, esito_raccolta, causa_manc_raccolta, mod_alt_racc, \
               dir_indennizzo, pros_fin

