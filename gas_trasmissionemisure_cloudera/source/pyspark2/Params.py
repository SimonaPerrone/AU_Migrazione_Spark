class Params:
    def __init__(self, flusso, anno, mese, giorno,
                 query=None,
                 query_file=None,
                 mode='yarn-client',
                 verbose=False,
                 enableTestMode=False):
        self.flusso = flusso
        self.anno = anno
        self.mese = mese
        self.giorno = giorno
        self.query = query
        self.query_file = query_file
        if not mode:
           mode='yarn-client'

        self.mode = mode
        self.verbose = verbose
        self.enableTestMode = enableTestMode
        pass

    def setFlusso(self, flusso):
        self.flusso = flusso

    def setAnno(self, anno):
        self.anno = anno

    def setMese(self, mese):
        self.mese = mese

    def setGiorno(self, giorno):
        self.giorno = giorno

    def setQuery(self, query):
        self.query = query

    def setQueryFile(self, query_file):
        self.query_file = query_file

    def setMode(self, mode):
        self.mode = mode

    def setVerbose(self, verbose):
        self.verbose = verbose

    def setTestMode(self, enableTestMode):
        self.enableTestMode = enableTestMode

    def print_debug(self):
        print("""
        Parameters: 
            Verbose    (v):\t{}
            Test Mode  (t):\t{}
            Mode       (m):\t{} 
            Flusso     (f):\t{}
            Query      (q):\t{}
            Query File (e):\t{}
            Anno          :\t{}
            Mese          :\t{}
            Giorno        :\t{}
            """.format(
            self.verbose,
            self.enableTestMode,
            self.mode,
            self.flusso,
            self.query,
            self.query_file,
            self.anno,
            self.mese,
            self.giorno))

