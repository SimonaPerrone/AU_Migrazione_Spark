import datetime
import os
from pyspark.sql.functions import *
from pyspark.sql.functions import expr, lit, monotonicallyIncreasingId
from pyspark.sql.types import *
import hashlib


class FlussoA40_0150:
    def __init__(self):
        self.XSD_SCHEMAS_DIR = "file:///mnt/isilonshare1/XSD_GAS/"
        self.PATH_HDFS = "/user/hive/warehouse/au.db/misure_gas_au/cmg_gas/prt_cmg_a40_0150_p"
        self.CMD_REFRESH = "MSCK REPAIR TABLE cmg_gas.prt_cmg_A40_0150_p"

    def getCodFlusso(self):
        return "A40_0150"

    def get_xsd_file(self, flusso):
        # TODO
        FILE_XSD = os.path.join(self.XSD_SCHEMAS_DIR, 'A/A40_0150.xsd')
        return FILE_XSD

    def write(self, rdd, sc, sqlCtx, test_mode_enabled):
        PARTITIONS = ["annomese"]

        schema = StructType([
            StructField("cod_prat_distr", StringType(), True),
            StructField("cod_prat_utente", StringType(), True),
            StructField("piva_utente", StringType(), True),
            StructField("piva_distr", StringType(), True),
            StructField("esito", StringType(), True),
            StructField("anno_fabb_mis", StringType(), True),
            StructField("matr_conv", StringType(), True),
            StructField("data_attivazione", StringType(), True),
            StructField("segn_mis", StringType(), True),
            StructField("segn_conv", StringType(), True),
            StructField("matr_mis", StringType(), True),
            StructField("cod_pdr", StringType(), True),
            StructField("note", StringType(), True),
            StructField("cod_servizio", StringType(), True),
            StructField("cod_flusso", StringType(), True),
            StructField("local_file", StringType(), True),
        ])

        n_id_file_udf = udf(
            lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
        )

        filename_udf = udf(
            lambda d: os.path.basename(d)
        )

        anno_udf = udf(
            lambda x: "EE" if x is None else str(x.split("/")[7])
        )

        mese_udf = udf(
            lambda x: "EE" if x is None else str(x.split("/")[8][:2])
        )

        annomese_udf = udf(
            lambda x: "EE" if x is None else str(x[6:10] + x[3:5])
        )

        df = sqlCtx.createDataFrame(rdd, schema=schema)

        df = df.withColumn("local_file", expr("regexp_replace(local_file, 'file:', '')"))
        df = df.withColumn("t_name_file", filename_udf(col('local_file')))
        df = df.withColumn("n_id_file", n_id_file_udf(col('local_file')))
        df = df.withColumn("n_id", n_id_file_udf(col('t_name_file')))

        dataElaborazione = str(datetime.datetime.now().isoformat())
        df = df.withColumn("d_caricamento", lit(dataElaborazione))
        df = df.withColumn("annomese", annomese_udf(col('data_attivazione')))
        df = df.withColumn("local_file", expr("regexp_replace(local_file, '/isilonshare_gas', '')"))

        df = df.withColumn("anno", anno_udf(col('local_file')))
        df = df.withColumn("mese", mese_udf(col('local_file')))

        path_hdfs = "{}{}".format(
            self.PATH_HDFS,
            "_test" if test_mode_enabled else "")
        cmd_refresh = "{}{}".format(
            self.CMD_REFRESH,
            "_test" if test_mode_enabled else "")

        print("Write: {}".format(path_hdfs))
        print("Partition List: {}".format(PARTITIONS))
        df.write.partitionBy(PARTITIONS).parquet(path_hdfs, 'append')
        # df.write.parquet(path_hdfs, 'append')
        sqlCtx.sql(cmd_refresh)
        pass

    def getItems(self, filename, content_xml):
        result = []
        cod_prat_distr, cod_prat_utente, piva_utente, piva_distr, esito, anno_fabb_mis, matr_conv, \
        data_attivazione, segn_mis, segn_conv, matr_mis, cod_pdr, note, cod_servizio, cod_flusso = self.reset()

        # IdentificativiRichiesta
        cod_prat_distr = None if content_xml.find(
            "IdentificativiRichiesta/cod_prat_distr") is None else content_xml.find(
            "IdentificativiRichiesta/cod_prat_distr").text
        cod_prat_utente = None if content_xml.find(
            "IdentificativiRichiesta/cod_prat_utente") is None else content_xml.find(
            "IdentificativiRichiesta/cod_prat_utente").text
        piva_utente = None if content_xml.find("IdentificativiRichiesta/piva_utente") is None else content_xml.find(
            "IdentificativiRichiesta/piva_utente").text
        piva_distr = None if content_xml.find("IdentificativiRichiesta/piva_distr") is None else content_xml.find(
            "IdentificativiRichiesta/piva_distr").text

        # Esito
        esito = None if content_xml.find("Esito") is None else content_xml.find("Esito").text

        # DatiTecnici
        anno_fabb_mis = None if content_xml.find("DatiTecnici/anno_fabb_mis") is None else content_xml.find(
            "DatiTecnici/anno_fabb_mis").text
        matr_conv = None if content_xml.find("DatiTecnici/matr_conv") is None else content_xml.find(
            "DatiTecnici/matr_conv").text
        data_attivazione = None if content_xml.find("DatiTecnici/data_attivazione") is None else content_xml.find(
            "DatiTecnici/data_attivazione").text
        segn_mis = None if content_xml.find("DatiTecnici/segn_mis") is None else content_xml.find(
            "DatiTecnici/segn_mis").text
        segn_conv = None if content_xml.find("DatiTecnici/segn_conv") is None else content_xml.find(
            "DatiTecnici/segn_conv").text
        matr_mis = None if content_xml.find("DatiTecnici/matr_mis") is None else content_xml.find(
            "DatiTecnici/matr_mis").text
        cod_pdr = None if content_xml.find("DatiTecnici/cod_pdr") is None else content_xml.find(
            "DatiTecnici/cod_pdr").text

        note = None if content_xml.find("note") is None else content_xml.find("note").text
        cod_servizio = None if content_xml.get("cod_servizio") is None else str(content_xml.get("cod_servizio"))
        cod_flusso = None if content_xml.get("cod_flusso") is None else str(content_xml.get("cod_flusso"))

        result.append(
            (
                cod_prat_distr, cod_prat_utente, piva_utente, piva_distr, esito, anno_fabb_mis, matr_conv,
                data_attivazione, segn_mis, segn_conv, matr_mis, cod_pdr, note, cod_servizio, cod_flusso, filename
            )
        )

        return result

    def reset(self):
        # IdentificativiRichiesta
        cod_prat_distr = None
        cod_prat_utente = None
        piva_utente = None
        piva_distr = None

        # Esito
        esito = None

        # DatiTecnici
        anno_fabb_mis = None
        matr_conv = None
        data_attivazione = None
        segn_mis = None
        segn_conv = None
        matr_mis = None
        cod_pdr = None

        note = None
        cod_servizio = None
        cod_flusso = None

        return cod_prat_distr, cod_prat_utente, piva_utente, piva_distr, esito, anno_fabb_mis, matr_conv, \
               data_attivazione, segn_mis, segn_conv, matr_mis, cod_pdr, note, cod_servizio, cod_flusso

