import logging
import sys
from IngestionIM1 import Ingestion
from Unzip import Unzip
from LoadProfStd import ProfStandard
from flussi.funFlussi import FunFlussi
from datetime import datetime


class Command:

    def __init__(self):
        """
        Inizializza parametri di default
        """
        self.zipCmd = False
        self.calcProfStandardMode = False
        self.mode = 'yarn-client'
        self.calcFunFlussi = False
        self.directoryName = ""
        self.dataI = ''
        self.dataF = ''
        self.codiceFlusso = ''
        self.file = ''
        self.giornoZIP = None
        self.annoZIP = None
        self.meseZIP = None
        self.distr = None

    def run(self):
        logging.debug('Run')

        if not self.directoryName:
            logging.error("Errore: directory o file di Input mancante")
            print("Errore: directory o file di Input mancante")
            sys.exit(2)

        if self.zipCmd == True:
            # Unzip file
            unzip = Unzip(
                file=self.directoryName,
                mode=self.mode,
                giorno=self.giornoZIP,
                mese=self.meseZIP,
                anno=self.annoZIP,
                codiceFlusso=self.codiceFlusso,
                distr=self.distr
            )
            unzip.start()
            logging.info("Modalita zip attiva")

        elif self.calcProfStandardMode == True:
            # Calcolo delle tabelle e constanti
            ProfStandard().start(inputFile=self.directoryName)
            logging.info("Calcolo profilo standard attivo")

        elif self.calcFunFlussi == True:
            # Controllo se il tipo flusso e' presente
            if self.typeFlusso == '':
                logging.error("Errore: tipo flusso mancante")
                print("Errore: tipo flusso mancante")
                sys.exit(2)

            # Controllo delle date Inizio e Fine
            if self.dataI == '' or self.dataF == '':
                logging.error("Errore: Intervallo Date non valido")
                print("Errore: dataI o dataF vuote o nulle")
                sys.exit(2)

            # Conversione data Inizio e data Fine
            dataI = datetime.strptime(self.dataI, '%Y-%m-%d').date()
            dataF = datetime.strptime(self.dataF, '%Y-%m-%d').date()
            logging.debug("DataI: ", dataI)
            logging.debug("DataF: ", dataF)

            # Controllo formato delle date Inizio e Fine
            if self.dataI == '' or self.dataF == '':
                logging.error("Errore: Conversione DataI o DataF non riuscita")
                print("Errore: Conversione DataI o DataF non riuscita")
                sys.exit(2)

            # CA (Complessa - f1)
            if self.typeFlusso == 'f1':
                funFlussi = FunFlussi(directoryName=self.directoryName, mode=self.mode)
                funFlussi.CalcoloConsumoAnnuoAltreFreq(dateStart=dataI, dateEnd=dataF)
                logging.info("Modalita calcolo consumo annuo (f1)")
            # CA (Semplice - f2)
            elif self.typeFlusso == 'f2':
                funFlussi = FunFlussi(directoryName=self.directoryName, mode=self.mode)
                funFlussi.CalcoloConsumoAnnuoMensileDettaglio(dateStart=dataI, dateEnd=dataF)
                logging.info("Modalita calcolo consumo annuo (f2)")
            # Prof_Standard - f3
            elif self.typeFlusso == 'f3':
                funFlussi = FunFlussi(directoryName=self.directoryName, mode=self.mode)
                funFlussi.CalcoloProfStandard(dateStart=dataI, dateEnd=dataF)
                logging.info("Modalita calcolo profilo standard")
            else:
                logging.error("Errore: Tipo flusso non valido")
                print("Errore: tipo flusso non valido")
                sys.exit(2)

        else:
            # Ingestione Flussi Misure Gas: RML, SW1, TAL, TGL, TML
            logging.info("Modalita ingestion dati")
            ingestion = Ingestion(
                directory_name=self.directoryName,
                mode=self.mode,
                cod_flusso=self.codiceFlusso,
                file=self.file,
                anno=self.annoZIP,
                mese=self.meseZIP,
                giorno=self.giornoZIP,
                distributore=self.distr
            )
            ingestion.start()

    def setDateStart(self, date):
        logging.info("Data Inizio: " + date)
        self.dataI = date

    def setDateEnd(self, date):
        logging.info("Data Fine: " + date)
        self.dataF = date

    def setFunFlussi(self, typeFlusso):
        self.typeFlusso = typeFlusso
        self.calcFunFlussi = True
        logging.debug('Set type Flusso %s', self.typeFlusso)

    def setDirectory(self, directory):
        self.directoryName = directory
        logging.debug('Set file input %s', self.directoryName)

    def unzip(self, zipCmd):
        self.zipCmd = zipCmd
        logging.debug('Set unzip function')

    def setMode(self, mode):
        self.mode = mode
        logging.debug('Set Mode' + mode)

    def calcProfStandard(self, calcProfStandardMode):
        self.calcProfStandardMode = calcProfStandardMode
        logging.debug('Set calc ProfStandard Mode')

    def setCodFlusso(self, codiceFlusso):
        self.codiceFlusso = codiceFlusso
        logging.debug('Set codiceFlusso', self.codiceFlusso)

    def setFile(self, file):
        self.file = file
        logging.debug('Set file', self.file)

    def setGiornoZip(self, giorno):
        self.giornoZIP = giorno
        logging.debug('Set giorno zip', self.giornoZIP)

    def setMeseZip(self, mese):
        self.meseZIP = mese
        logging.debug('Set mese zip', self.meseZIP)

    def setAnnoZip(self, anno):
        self.annoZIP = anno
        logging.debug('Set anno zip', self.annoZIP)

    def setDistr(self, distr):
        self.distr = distr
        logging.debug('Set distr', self.distr)




