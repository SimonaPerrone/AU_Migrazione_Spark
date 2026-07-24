#!/usr/bin/python

import getopt
import sys
import os
import logging
import logging.config
import constants


from Command import Command

def usage():
    print('Version: 1.0')
    print('Usage')
    print('-i: file di input (XML per file dati; CSV per tabella profStandard)')
    print('-v: verbose mode')
    print('-h: visualizza questa guida')
    print('-m: mode (local, yarn, yarn-client)')
    print('')

    print('Ingestion')
    print('-f oppure --flusso: flusso da importare')
    print('-e oppure --file: file da importare')
    print('Esempio:')
    print('Avviare modalita ingestion:')
    print('     spark-submit  --py-files /home/acu/AU/test_pyspark/gas.zip /home/acu/AU/test_pyspark/src/Main.py -i /mnt/share/data/GAS/XML/')
    print('')

    print('Unzip')
    print('-z: modalita zip')
    print('--giorno: Giorno nel formato <GG>')
    print('--mese: Mese; <MM>')
    print('--anno: Anno; <AA>')
    print('Esempio:')
    print('Avviare modalita decompres:')
    print('     spark-submit  --py-files /home/acu/AU/test_pyspark/gas.zip /home/acu/AU/test_pyspark/src/Main.py -z -i /mnt/share/data/GAS/TMP/')
    print('')

    #print('-s: modalita calcolo profili standard')
    #print('-c: calcola flusso dati (f1, f2, f3... )')
    #print('--dateStart: data; intervallo iniziale')
    #print('--dateEnd: data; intervallo finale')
    #print('Avviare modalita Calcolo Consumo Annuo (f1):')
    #print('     spark-submit  --py-files /home/acu/AU/test_pyspark/gas.zip /home/acu/AU/test_pyspark/src/Main.py -c f1 --dateStart=2018-10-02 --dateEnd=2018-12-16 -i /mnt/share/data/GAS/TMP/')

def main(argv):
    version = '1.0'

    # Controllo se la directory di LOG esiste
    if (os.path.isdir("./LOG") == False):
        logging.warning("Directory di Log non esiste")
        os.mkdir("./LOG")

    logging.config.fileConfig(constants.LOCAL_PATH + 'logging.ini')
    command = Command()

    logging.debug("Avvio Applicazione")

    try:
        opts, args = getopt.getopt(argv, "hvi:zm:sc:f:e:d:", ["input=", "zip=", "mode=", "profstd=", "calculate=", "dateStart=", "dateEnd=", "flusso=", "file=", "giorno=", "anno=", "mese=", "distr="])
    except getopt.GetoptError as err:
        logging.error(err)
        usage()
        sys.exit(2)

    #print (argv)
    for opt, arg in opts:
        print (opt,arg)
        logging.debug("Parameters: " + opt + " " + arg)

        if opt == '-h':
            usage()
            sys.exit()
        elif opt == '-v':
            print('Version: ', version)
            sys.exit()
        elif opt in ('-i', '--input'):
            command.setDirectory(arg)
        elif opt in ('-m', '--mode'):
            command.setMode(arg)
        elif opt in ('-z', '--zip'):
            command.unzip(True)
        elif opt in ('-s', '--profstd'):
	    command.calcProfStandard(calcProfStandardMode = True) 
        elif opt in ('-c', '--calculate'):
	    command.setFunFlussi(typeFlusso = arg) 
        elif opt == "-d" or opt == "--distr":
            command.setDistr(distr = arg)
        elif opt in ('--dateStart'):
	    command.setDateStart(date = arg)
        elif opt in ('--dateEnd'):
	    command.setDateEnd(date = arg)
        elif opt in ('-f', '--flusso'):
	    command.setCodFlusso(codiceFlusso = arg)
        elif opt in ('-e', '--file'):
	    command.setFile(file = arg)
        elif opt in ('--giorno'):
            command.setGiornoZip(giorno = arg)
        elif opt in ('--anno'):
            command.setAnnoZip(anno = arg)
        elif opt in ('--mese'):
            command.setMeseZip(mese = arg)
        
    #return
    command.run()


if __name__ == "__main__":
    main(sys.argv[1:])
