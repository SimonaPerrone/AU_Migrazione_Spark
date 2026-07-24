import getopt
import sys
import os
import json
from REQs.classes.functions.switcher import switcher
from functions.spark_utils import *
from pyspark.sql.utils import IllegalArgumentException
import shutil
import datetime
import pyspark.sql.functions as F
import time
from pyspark.sql import Window


def main(argv):
    version = '1.0'
    directory_input = ""
    mode = 'yarn-client'

    try:
        opts, args = getopt.getopt(argv, "hvi:f:m:", ["input=", "function=", "mode="])
    except getopt.GetoptError as err:
        sys.exit(2)

    # -i, input:        Directory di input
    # -v:               Versione
    # -f, function:     Funzione (indice)

    for opt, arg in opts:
        print(opt, arg)

        if opt == '-h':
            # TODO usage()
            sys.exit()
        elif opt == '-v':
            print('Version: ', version)
            sys.exit()
        elif opt in ('-i', '--input'):
            directory_input = arg
        elif opt in ('-f', '--function'):
            function_input = arg
        elif opt in ('-m', '--mode'):
            mode = arg

    # Verificare se directory_input e' una directory esistente
    if (not checkDirectoryInput(directory_input)):
        print("Error: directory input not exist or not valid")
        sys.exit(1)

    path = os.path.join(os.getcwd(), "../conf/functions.json")
    config_jobs = load_config(path)

    # Creare spark context
    sc, sqlCtx = set_spark_context("GAS - Function: " + function_input, mode)
    sqlCtx.setConf("spark.dynamicAllocation.minExecutors", "16")

    # Recupera l'oggetto function
    function = switcher(function_input, conf=config_jobs)

    # Apertura file di input
    directory_input_files = directory_input + "/input.in"
    listFiles = []
    f = open(directory_input_files, "r")
    for x in f:
        directory = x.replace('\n', '')
        try:
            listFiles.append(getFiles(directory))
        except:
            print("la Directory non esiste", directory)

    # flat la lista dei files
    listFiles = [y for x in listFiles for y in x]

    # Recuperare tutti i file all'interno della directory
    # listFiles = getFiles(directory_input)

    # rdd = sc.parallelize(listFiles).map(lambda f: "file://" + f)

    # function.backup_table(sqlCtx)

    # function.load_tables(sqlCtx)
    function.backup_table_tmp(sqlCtx)

    listElaborate = []
    for f in listFiles:
        salto_file = False

        # Controllare se il file e' AMM
        if "AMM" in f:
            print("Salto file; file di output", f)
            continue

        if ".part" in f:
            print("Salto file; file di output", f)
            continue

        # Controllare se ce' un file di tipo AMM
        check_file = f.replace(".csv", "") + "_AMM"
        for item in listFiles:
            if len(item) > len(check_file):
                if item[:len(check_file)] == check_file:
                    print("Salto file gia elaborato: ", f)
                    salto_file = True
                    break

        if salto_file:
            continue
        # print ("Elabora file", f)

        # Verifica validazione file
        result_validazione_file = function.validate_filename(f)

        if result_validazione_file:
            print("File valido", f)
            listElaborate.append(f)
            pass
        else:
            # Scrittura del risultato della validazione errata
            now = datetime.datetime.now()
            text = "200;Il file non rispetta la struttura prevista - Nome del file non conforme"
            time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
            w_filename = f.replace(".csv", "") + "_AMM_" + time_stamp + ".csv"

            l = [("200", "Il file non rispetta la struttura prevista - Nome del file non conforme", f, 0)]
            dataframe_KO = sqlCtx.createDataFrame(l, function.getSchema_ANOMALIE_FILE())
            function.save_data_anomalie_file(sqlCtx, dataframe_KO)

            function.save_to_file(w_filename, text)
            print("File non valido", f)
            pass

        continue

    if not listElaborate:
        print("Nessun file da elaborare")
        return

    # Parallize la listElaborate
    rdd = sc.parallelize(listElaborate)
    # rdd.foreach(stampa)

    textFiles = sc.wholeTextFiles(",".join(f for f in rdd.map(lambda f: "file://" + f).collect()), minPartitions=20)
    vflusso = sqlCtx.createDataFrame(textFiles, ['file', 'value'])

    rdd_tb = vflusso.map(lambda d: (d[0], [x for x in d[1].replace("\r", "").split('\n') if len(x.strip()) > 0]))
    vflusso2 = sqlCtx.createDataFrame(rdd_tb, ['file', 'value'])

    exploded_df = vflusso2.select("*", F.explode("value").alias("text"))
    # exploded_df.show()

    # print exploded_df.count()
    w = Window().partitionBy('file').orderBy(lit('A'))
    df = exploded_df.withColumn("row_num", row_number().over(w))

    # rdd2 = df.map(lambda d: (d[0].replace("file:",""), str(d[3]) + ";" + str(d[2].encode('utf-8')) ))
    rdd2 = df.map(lambda d: (d[0].replace("file:", ""), d[3], d[2]))

    # rdd.map(lambda d: (d, elaborate(d))).toDF().show()
    # rdd2.toDF().show(truncate=False)

    rddCheckDefinitionHeader = rdd2.filter(lambda d: d[1] == 1) \
        .map(lambda d: (d[0], str(str(d[1]) + ";" + str(d[2].encode('utf-8'))))) \
        .map(lambda d: (function.check_definition_record(d[1]), d[0]))

    # rddCheckDefinitionHeader.toDF().show(truncate = False)
    # rdd2.toDF().show(truncate = False)

    df2 = sqlCtx.createDataFrame(rdd2, ['file', 'n_row', 'value'])
    dfCheckDefinitionHeader = sqlCtx.createDataFrame(rddCheckDefinitionHeader, ['valid', 'file'])

    # df2.join(dfCheckDefinitionHeader, df2.file == dfCheckDefinitionHeader.file )\
    #   .filter(dfCheckDefinitionHeader.valid == True)\
    #   .map(lambda d: (d[0], str(str(d[1]) + ";" + str(d[2]))))

    # df.show()

    # for x in df.map(lambda d: (d[0], str(d[3]) + ";" + str(d[2]) )).collect():
    #     print x
    # return

    # map per ogni elemento: (il map deve ritornare degli elemento cosi formati: (nome_file, [tracciato]) )
    #   apertura file (Apertura deve essere fatta tramite funzione in OS)
    #   eseguire un mapflat sul secondo record, recuperando i dati
    time1 = time.time()

    # rdd2 = rdd.map(lambda d: (d, elaborate(d)))
    # rddCheckDefinitionHeader = rdd2.map(lambda d: (function.check_definition_record(d[1][0]), d))

    rddCheckDefinitionHeaderGood = df2.join(dfCheckDefinitionHeader, df2.file == dfCheckDefinitionHeader.file) \
        .filter(dfCheckDefinitionHeader.valid == True) \
        .map(lambda d: (d[0], str(str(d[1]) + ";" + str(d[2].encode('utf-8')))))

    rddCheckDefinitionHeaderBad = rddCheckDefinitionHeader \
        .filter(lambda f: f[0] == False) \
        .map(lambda f: ("200", "Il file non rispetta la struttura prevista - Nome del file non conforme", f[1], 0))
    ### INIZIO -------- NUOVA PARTE

    # Preleva i file con header buono
    # rddCheckDefinitionHeaderGood = rddCheckDefinitionHeader\
    #           .filter(lambda f: f[0] == True)\
    #           .map(lambda f: f[1])

    # rddCheckDefinitionHeaderBad = rddCheckDefinitionHeader\
    #           .filter(lambda f: f[0] == False)\
    #           .map(lambda f: ("200", "Il file non rispetta la struttura prevista - Nome del file non conforme", f[1][0], 0))

    if not rddCheckDefinitionHeaderBad.isEmpty():
        for row in rddCheckDefinitionHeaderBad.collect():
            now = datetime.datetime.now()
            text = row[0] + ";" + row[1]
            time_stamp = str(now.year).zfill(4) + "" + str(now.month).zfill(2) + "" + str(now.day).zfill(2)
            w_filename = row[2].replace(".csv", "") + "_AMM_" + time_stamp + ".csv"

            print("scrittura sul file non valido:", w_filename)
            function.save_to_file(w_filename, text)

        dataframe_KO = sqlCtx.createDataFrame(rddCheckDefinitionHeaderBad, function.getSchema_ANOMALIE_FILE())
        function.save_data_anomalie_file(sqlCtx, dataframe_KO)

    if rddCheckDefinitionHeaderGood.isEmpty():
        print("Nessun data da elaborare")
        return

    df = rddCheckDefinitionHeaderGood.toDF()

    # exploded_df = df.select("*", F.explode("_2").alias("text"))
    exploded_df = df

    # exploded_df.show(truncate = False)
    # Valida i record
    rdd_items_result = exploded_df.map(lambda f: function.valida_record(f[1], f[0]))

    # preleva quelli buoni
    rdd2 = rdd_items_result.filter(lambda f: f[0] == True)

    # preleva quelli da scartare (non include il primo record -> header)
    rdd3 = rdd_items_result.filter(lambda f: f[0] == False) \
        .filter(lambda f: f[5] != "1") \
        .map(lambda f: (f[3], f[4], f[1], f[5]))

    dataframe_OK = sqlCtx.createDataFrame(rdd2, function.getSchema_AMMISSIBILITA_FILE())
    dataframe_KO = sqlCtx.createDataFrame(rdd3, function.getSchema_ANOMALIE_FILE())

    time2 = time.time()
    print("Lettura files completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    time1 = time.time()
    function.save_data_anomalie(sqlCtx, dataframe_KO)
    time2 = time.time()
    print("Scrittura Anomalie in %0.3f ms" % ((time2 - time1) * 1000.0))

    rcugas_massivo_dataframe, dataframe_rcuazienda, rcugas_connessioni_distr, rcugas_tmp_va1 = function.load_tables(
        sqlCtx)

    print("Controllo duplicati")
    time1 = time.time()
    dataframe_OK, dataframe_KO_duplicate = function.get_duplicate(sqlCtx, dataframe_OK)
    function.save_data_anomalie(sqlCtx, dataframe_KO_duplicate)
    time2 = time.time()
    print("Controllo duplicati completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    print("check rcu filename")
    time1 = time.time()
    dataframe_OK, dataframe_KO_rcu_file = function.check_rcu_filename(sqlCtx, dataframe_OK, dataframe_rcuazienda)
    function.save_data_anomalie(sqlCtx, dataframe_KO_rcu_file)

    time2 = time.time()
    print("Controllo filename completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    print("check Il PdR e' inesistente")
    time1 = time.time()
    dataframe_OK, dataframe_KO_1 = function.get_rcu_1(sqlCtx, dataframe_OK, rcugas_massivo_dataframe)
    function.save_data_anomalie(sqlCtx, dataframe_KO_1)

    time2 = time.time()
    print("Controllo inesistenza completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    print("check Il PdR non e' attivo")
    time1 = time.time()
    dataframe_OK, dataframe_KO_2 = function.get_rcu_2(sqlCtx, dataframe_OK, rcugas_massivo_dataframe)
    function.save_data_anomalie(sqlCtx, dataframe_KO_2)

    time2 = time.time()
    print("Controllo attivo completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    print("check extra")
    time1 = time.time()
    dataframe_OK, dataframe_KO_extra = function.extra(sqlCtx, dataframe_OK, rcugas_massivo_dataframe,
                                                      rcugas_connessioni_distr, rcugas_tmp_va1)
    function.save_data_anomalie(sqlCtx, dataframe_KO_extra)

    time2 = time.time()
    print("Controllo extra completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    print("check rcu UdD")
    time1 = time.time()
    dataframe_OK, dataframe_KO_rcu_udd = function.get_rcu_rcu_udd(sqlCtx, dataframe_OK, rcugas_massivo_dataframe)
    function.save_data_anomalie(sqlCtx, dataframe_KO_rcu_udd)

    time2 = time.time()
    print("Controllo UdD completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    print("Save ammissibilita")
    function.save_data_ammissiblita(sqlCtx, dataframe_OK)
    print("salvataggio ammissibilita completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    time1 = time.time()
    dataframe_union = function.generate_dataframe_to_csv(sqlCtx)
    dataframe_union = dataframe_union.orderBy('num_riga')
    time2 = time.time()
    print("Creazione tabella Ammissiblita completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    # dataframe_union.show()
    time1 = time.time()
    function.save_to_csv(sqlCtx, dataframe_union)
    time2 = time.time()
    print("Salvataggio csv completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    time1 = time.time()
    # function.save_data(sqlCtx, dataframe_OK )
    time2 = time.time()
    print("Salvataggio dati completata in %0.3f ms" % ((time2 - time1) * 1000.0))

    # FINE --- NUOVA PARTE

    return


def debug(function, sqlCtx, dataframe_OK, dataframe_KO):
    function.save_data_ammissiblita(sqlCtx, dataframe_OK)
    dataframe_OK.show()
    dataframe_KO.show()
    return


def getHeader(text):
    print("text:", text)
    items = record.split(';')
    print("items:", items)
    if (items[0] == 1):
        return True

    return False


def elaborate(filename):
    # print ("Elaborate filename:", filename)
    file = open(filename, "r")
    items_result = []

    for index, item in enumerate(file):
        items_result.append(str(index + 1) + ";" + item.replace("\r", "").replace("\n", ""))

    return items_result


def RemoveFolder(directory):
    print("Delete file in directory:", directory)

    for tmp_dir in os.listdir(directory):
        to_delete = os.path.join(directory, tmp_dir)
        os.remove(to_delete)


def expand(row):
    result = []
    file_name = row[0]

    for item in row[1]:
        result.append((row[0], item))

    return result


def stampa(value):
    try:
        print("***** ", value)
    except:
        print("***** EEE")


def load_config(path):
    """
    Load configuration file with all the needed parameters
    """
    with open(path, 'r') as conf_file:
        conf = json.load(conf_file)
    return conf


def checkDirectoryInput(directorySRC):
    result = os.path.isdir(directorySRC)

    return result


def getFiles(directorySRC):
    files = [os.path.join(directorySRC, f) for f in os.listdir(directorySRC)]
    return files


if __name__ == "__main__":
    main(sys.argv[1:])
