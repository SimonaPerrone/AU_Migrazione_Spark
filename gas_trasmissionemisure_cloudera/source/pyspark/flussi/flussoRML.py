import constants
import hashlib
import datetime
import os
import re

from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.types import *

class FlussoRML:
    def __init__(self, codFlusso):
        self.codFlusso = codFlusso

    def isValidFile(self, filename):
        """
        Validazione del file xml
        :param filename: nome del file xml da verificare
        :return: ritorna booleano; True e' un elemento valido altrimenti False
        """
        pattern = "^\\w{11,16}_\\w{11,16}_\\d{6}_(R|r)(M|m)(L|l)(\\d{4}|.\\d{4})_\\d*(_\\d{1,}|).\\w{3}$"
        return bool(re.match(pattern, filename))

    def makeError(self, dataElaborazione, anno, mese, local_file):
        """
        Crea un oggetto vuoto. Utilizzare questa funzione quando si genera un errore
        :param data_elaborazione: Data elaborazione
        :param anno: anno
        :param mese: mese
        :param local_file: nome del file da elaborare
        :return: ritorna lista con un elemento vuoto
        """
        result = []

        #codice_servizio, cod_flusso, piva_utente, piva_distr, cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, \
        #data_comp, data_racc, let_tot_prel, let_tot_conv, mot_rett_lett, \
        #vol_ric, ini_periodo, fine_periodo = self.reset()

        codice_servizio = ""
        piva_utente=""
        piva_distr=""
        cod_flusso =""

        cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, \
        data_comp, data_racc, let_tot_prel, let_tot_conv, mot_rett_lett, \
        vol_ric, ini_periodo, fine_periodo = self.reset()

        result.append(
            (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr,
             cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, data_comp, data_racc, let_tot_prel,
             let_tot_conv, mot_rett_lett, vol_ric, ini_periodo, fine_periodo, anno, mese, local_file)
        )

        return result

    def getFileXSD(self, codice_servizio, codice_flusso):
        """
        Ritorna il nome del file XSD da utilizzare per la validazione
        :param codice_servizio: nome del codice servizio
        :param codice_flusso: nome del codice flusso
        :return: ritorna il nome del file XSD
        """
        file_xsd = ""
        # Flusso RML
        if codice_servizio == constants.CODSERVIZIO_RML and codice_flusso == "0055":
            file_xsd = constants.FILE_XSD_RML_0055
        elif codice_servizio == constants.CODSERVIZIO_RML and codice_flusso == "0056":
            file_xsd = constants.FILE_XSD_RML_0056
        return file_xsd

    def write(self, rdd, pool_id, sc, sqlCtx):
        """
        Scrittura degli RDD all'interno della tabella
        :param rdd: rdd da elaborare e scrivere
        :param pool_id: identificativo univoco, se si vuole eseguire questa funzione all'interno di un thread
        :param sc: spark context
        :param sql_ctx: sql context
        :return:
        """
        print("Thread RML")
        sc.setLocalProperty("spark.scheduler.pool", str(pool_id))
        dfRML = sqlCtx.createDataFrame(rdd, schema=constants.schemaFlussoRML_0055)
        #dfRML = dfRML.withColumn("data_comp", expr("from_unixtime(unix_timestamp(data_comp, 'dd/MM/yyyy'))").cast("timestamp"))
        #dfRML = dfRML.withColumn("data_racc", expr("from_unixtime(unix_timestamp(data_racc, 'dd/MM/yyyy'))").cast("timestamp"))

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        #data_racc gg/mm/aaaa
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

        dfRML = dfRML.withColumn("t_name_file",  filename_udf(col('local_file')))
        dfRML = dfRML.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        dfRML = dfRML.withColumn("annomese_riferimento", annomese_rif_udf(col('local_file')))
        dfRML = dfRML.withColumn("annomese", annomese_udf(col('data_racc')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        dfRML = dfRML.withColumn("d_caricamento",  lit(dataElaborazione))
        dfRML = dfRML.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        #TODO Set partition list ---> constants.PARTITIONLIST_RML
        print("Write: ", constants.PATHDFS_RML)
        print("Partition List: ", constants.PARTITIONLIST_RML)
        dfRML.write.partitionBy(constants.PARTITIONLIST_RML).parquet(constants.PATHDFS_RML, 'append')
        sqlCtx.sql(constants.CMD_REFRESH_RML)
        sc.setLocalProperty("spark.scheduler.pool", None)

    def getItems(self, root, dataElaborazione, anno, mese, local_file):
        """
        Elaborazione del file xml, recuperando l'elemento da inserire nella tabella
        :param root: elemento principale del nodo xml
        :param dataElaborazione: data elaborazione
        :param anno: anno
        :param mese: mese
        :param local_file: nome del file xml
        :return: Ritorna un array degli elementi individuati nel file
        """
        result = []
        cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, \
        data_comp, data_racc, let_tot_prel, let_tot_conv, mot_rett_lett, \
        vol_ric, ini_periodo, fine_periodo = self.reset()

	print("root:",root)
        if root is None:
            print("Warn", "Root is None")
            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr,
                 cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, data_comp, data_racc, let_tot_prel,
                 let_tot_conv, mot_rett_lett, vol_ric, ini_periodo, fine_periodo, anno, mese, local_file)
            )
            return result
 
        codice_servizio = None if root.get("cod_servizio") is None else root.get("cod_servizio")
        cod_flusso = None if root.get("cod_flusso") is None else root.get("cod_flusso")

        piva_utente = None if root.find("IdentificativiRichiesta/piva_utente") is None else root.find("IdentificativiRichiesta/piva_utente").text
        piva_distr = None if root.find("IdentificativiRichiesta/piva_distr") is None else root.find("IdentificativiRichiesta/piva_distr").text


        if anno == "" or mese == "" or piva_distr == "" or piva_utente == "":
            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr,
                 cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, data_comp, data_racc, let_tot_prel,
                 let_tot_conv, mot_rett_lett, vol_ric, ini_periodo, fine_periodo, "EE", "EE", local_file)
            )

            return result

        DatiPdR = root.findall("DatiPdR")

        if DatiPdR == None:
            print("Warn", "flusso RML non valido")
            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr,
                 cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, data_comp, data_racc, let_tot_prel,
                 let_tot_conv, mot_rett_lett, vol_ric, ini_periodo, fine_periodo, anno, mese, local_file)
            )

            return result

        for item in DatiPdR:
            cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let,\
            data_comp, data_racc, let_tot_prel, let_tot_conv, mot_rett_lett, \
            vol_ric, ini_periodo, fine_periodo = self.reset()

            cod_pdr = None if item.find("cod_pdr") is None else item.find("cod_pdr").text
            matr_mis = None if item.find("matr_mis") is None else item.find("matr_mis").text
            matr_conv = None if item.find("matr_conv") is None else item.find("matr_conv").text
            coeff_corr = None if item.find("coeff_corr") is None else item.find("coeff_corr").text
            freq_let = None if item.find("freq_let") is None else item.find("freq_let").text
            data_comp = None if item.find("data_comp") is None else item.find("data_comp").text
            data_racc = None if item.find("data_racc") is None else item.find("data_racc").text
            let_tot_prel = None if item.find("let_tot_prel") is None else item.find("let_tot_prel").text
            let_tot_conv = None if item.find("let_tot_conv") is None else item.find("let_tot_conv").text
            mot_rett_lett = None if item.find("mot_rett_lett") is None else item.find("mot_rett_lett").text

            vol_ric = None if item.find("vol_ric") is None else item.find("vol_ric").text
            ini_periodo = None if item.find("ini_periodo") is None else item.find("ini_periodo").text
            fine_periodo = None if item.find("fine_periodo") is None else item.find("fine_periodo").text

            result.append(
                (codice_servizio, cod_flusso, dataElaborazione, piva_utente, piva_distr,
                 cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, data_comp, data_racc, let_tot_prel,
                 let_tot_conv, mot_rett_lett, vol_ric, ini_periodo, fine_periodo, anno, mese, local_file)
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
        freq_let = None
        data_comp = None
        data_racc = None
        let_tot_prel = None
        let_tot_conv = None
        mot_rett_lett = None
        vol_ric = None
        ini_periodo = None
        fine_periodo = None

        return cod_pdr, matr_mis, matr_conv, coeff_corr, freq_let, data_comp, data_racc, let_tot_prel, let_tot_conv, \
               mot_rett_lett, vol_ric, ini_periodo, fine_periodo

