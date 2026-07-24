import os
import glob
import constants

class UtilFiles:

    def getListOfFilesByAnno(self, baseDirName, anno, ext):
        """
            Ritorna la lista dei file di ogni distributore filtrando per anno
        """
        listOfDistr = os.listdir(baseDirName)
        allFiles = list()
        
        # List file primo livello (Distributore)
        for entry in listOfDistr:
            listUtenti = os.listdir(os.path.join(baseDirName, entry, "DISTRIBUTORE"))
            #print("utils: getListOfFilesByAnno:", listUtenti)
            for item_utenti in listUtenti:
                #print("utils: getListOfFilesByAnno:", item_utenti)
                items_mesigiorni = os.listdir(os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti, anno))
                for item_files in items_mesigiorni: 
                    #print(item_files)
                    files = os.listdir( os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, item_files))
                    #allFiles.append([ item for item in files if os.path.isfile(os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, item_files, item))])
                    allFiles.append([ os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, item_files, item) for item in files if os.path.isfile(os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, item_files, item))])
                    #print("utils: getListOfFilesByAnno:", allFiles)

                    #print("allFile:", allFiles)
        #[item for sublist in l for item in sublist]    
        return_list = [ item for sublist in allFiles for item in sublist]
        #print("returnList:", return_list)
        #return return_list
        return [ item for item in return_list if os.path.splitext(item)[1] == ext ]
        #return allFiles

    def getListOfFilesByAnnoMese(self, baseDirName, anno, mese, ext):
        """
            Ritorna la lista dei file di ogni distributore filtrando per anno
        """
        listOfDistr = os.listdir(baseDirName)
        allFiles = list()

        # List file primo livello (Distributore)
        for entry in listOfDistr:
            listUtenti = os.listdir(os.path.join(baseDirName, entry, "DISTRIBUTORE"))
            #print("utils: getListOfFilesByAnno:", listUtenti)
            for item_utenti in listUtenti:
                #print("utils: getListOfFilesByAnno:", item_utenti)
                items_mesigiorni = os.listdir(os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti, anno))
                for mesegiorno in items_mesigiorni:
                        mese_str = mesegiorno[:2]
                        if (mese_str == mese):
                                files = os.listdir( os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, mesegiorno))
                                allFiles.append([ os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, mesegiorno, item) for item in files if os.path.isfile(os.path.join(baseDirName, entry, "DISTRIBUTORE", item_utenti ,anno, mesegiorno, item))])

        #print("allFile:", allFiles)
        #[item for sublist in l for item in sublist]
        return_list = [ item for sublist in allFiles for item in sublist]
        #print("returnList:", return_list)
        #return return_list
        return [ item for item in return_list if os.path.splitext(item)[1] == ext ]


    def getListOfFilesByAnnoMeseDistr(self, baseDirName, anno, mese, distributore, ext):
        """
            Ritorna la lista dei file di ogni distributore filtrando per anno
        """
        listOfDistr = os.listdir(baseDirName)
        allFiles = list()

        # List file primo livello (Distributore)
        listUtenti = os.listdir(os.path.join(baseDirName, distributore, "DISTRIBUTORE"))
        #print("utils: getListOfFilesByAnno:", listUtenti)
        for item_utenti in listUtenti:
            #print("utils: getListOfFilesByAnno:", item_utenti)
            items_mesigiorni = os.listdir(os.path.join(baseDirName, distributore, "DISTRIBUTORE", item_utenti, anno))
            for mesegiorno in items_mesigiorni:
                mese_str = mesegiorno[:2]
                if (mese_str == mese):
                    files = os.listdir( os.path.join(baseDirName, distributore, "DISTRIBUTORE", item_utenti ,anno, mesegiorno))
                    allFiles.append([ os.path.join(baseDirName, distributore, "DISTRIBUTORE", item_utenti ,anno, mesegiorno, item) for item in files if os.path.isfile(os.path.join(baseDirName, distributore, "DISTRIBUTORE", item_utenti ,anno, mesegiorno, item))])

        #print("allFile:", allFiles)
        #[item for sublist in l for item in sublist]
        return_list = [ item for sublist in allFiles for item in sublist]
        #print("returnList:", return_list)
        return [ item for item in return_list if os.path.splitext(item)[1] == ext ]



   # Versione V3: usata per recuperare la lista delle sottodirectory di un particolare distributore
    def getListOfFiles3V(self, dirName, anno, mese, giorno, codiceFlusso, distr):
        listOfFile = os.listdir(dirName)
        allFiles = list()
        entry = os.path.join(dirName, distr)
        listOfFile2 = os.listdir(os.path.join(dirName, entry, "DISTRIBUTORE"))
        for entry2 in listOfFile2:
            allFiles.append(os.path.join(dirName, entry, "DISTRIBUTORE",entry2))
            
        return allFiles

    def getListOfFiles2V(self, dirName, anno, mese, giorno, codiceFlusso):
        listOfFile = os.listdir(dirName)
        allFiles = list()
        
        # List file primo livello (Distributore)
        for entry in listOfFile:
            listOfFile2 = os.listdir(os.path.join(dirName, entry, "DISTRIBUTORE"))
            for entry2 in listOfFile2:
                allFiles.append(os.path.join(dirName, entry, "DISTRIBUTORE",entry2))
            
        return allFiles

    def getListOfFiles2V_parallel(self, sc, dirName, anno, mese, giorno, codiceFlusso):
        """
            Ritorna la lista dei file in formato RDD
            sc - spark context
            dirName - directory root di input
            anno
            mese
            giorno
            codiceFlusso - codice flusso da filtrare

            Il numero di partizioni 30
        """
        list_dir = os.listdir(dirName)
        number_partition = 30
        rddDirectory = sc.parallelize(list_dir)
        df = rddDirectory.map(lambda x: (x,)).toDF().repartition(number_partition)

        #df.show(truncate = False)
        rdd2Directory = df.map(lambda x: self.findItem(dirName + x[0] + "/DISTRIBUTORE/", codiceFlusso))
        return rdd2Directory


    def getListOfFiles2V_parallel2(self, sc, dirName, anno, mese, giorno, codiceFlusso, distr):
        """
            Ritorna la lista dei file in formato RDD
            sc - spark context
            dirName - directory root di input
            anno
            mese
            giorno
            codiceFlusso - codice flusso da filtrare
            distr  - distributore

            Il numero di partizioni 30
        """

        sub_structure_distr = "*"
        if distr is not None:
            sub_structure_distr = str(distr)

        #list_dir = os.listdir(dirName)
        pattern = os.path.join(dirName, sub_structure_distr)
        print("Pattern 1: ", pattern)
        print("DirName: ", dirName)

        list_dir = glob.glob(pattern)

        #print("list_dir: {}".format(list_dir))

        number_partition = 30
        rdd_directory = sc.parallelize(list_dir)
        df = rdd_directory.map(lambda x: (x,)).toDF().repartition(number_partition)

        #df.show(truncate = False)
        rdd_result = df.map(lambda x: self.findItemV2( x[0] + "/DISTRIBUTORE/", anno, mese, giorno, codiceFlusso))
        return rdd_result

    def findItem(self, dirName, codiceFlusso):
        print("Elaborazione directory:", dirName)
        allFile = list()
        for entry in os.listdir(dirName):
            listOfFile2 = os.listdir(os.path.join(dirName, entry))
            for anno in listOfFile2:  #anno
                for mmgg in os.listdir(os.path.join(dirName, entry, anno)): #mese giorno
                    list_mmgg = os.path.join(dirName, entry, anno, mmgg)
                    list_files = os.listdir(os.path.join(list_mmgg))
                    if codiceFlusso == None:
                        allFile.append([ os.path.join(list_mmgg, item) for item in list_files if (os.path.isfile(os.path.join(list_mmgg, item)))] )
                    else:
                        allFile.append([ os.path.join(list_mmgg, item) for item in list_files if (os.path.isfile(os.path.join(list_mmgg, item)) and codiceFlusso in item)] )

        return_list = [ item for sublist in allFile for item in sublist]
        return return_list


    def findItemV2(self, dirName, anno, mese, giorno, codiceFlusso):
        allFile = list()

        sub_structure_anno = "*"
        sub_structure_mese = "*"
        sub_structure_codice_flusso = "*"

        if anno is not None:
            sub_structure_anno = str(anno)
        if mese is not None and giorno is None:
            sub_structure_mese = str(mese) + "*"
        if mese is not None and giorno is not None:
            sub_structure_mese = str(mese) + str(giorno)
        if mese is None and giorno is not None:
            sub_structure_mese = "*" + str(giorno)
        if codiceFlusso is not None:
            sub_structure_codice_flusso = "*" + codiceFlusso + "*" 

        sub_structure = "*/" + sub_structure_anno + "/" + sub_structure_mese + "/"  + sub_structure_codice_flusso 

        pattern = os.path.join(dirName, sub_structure)
        print ("Pattern: ", pattern)

        #/mnt/isilonshare_gas/TMG_01671350682/DISTRIBUTORE/*/**/*TMV*
        for entry in glob.glob(pattern):
            if (codiceFlusso.upper() in entry.upper()):
                if os.path.isfile(entry) :
                    allFile.append(entry)
        print ("Pattern: ", pattern, len(allFile))

        return allFile


    def getListFiles2(self, dirName, entry, anno, mmgg, codiceFlusso):
        list_mmgg = os.path.join(dirName, entry, anno, mmgg)
        list_files = os.listdir(os.path.join(list_mmgg))
        if codiceFlusso == None:
            return [ os.path.join(list_mmgg, item) for item in list_files if (os.path.isfile(os.path.join(list_mmgg, item)))]

        return [ os.path.join(list_mmgg, item) for item in list_files if (os.path.isfile(os.path.join(list_mmgg, item)) and codiceFlusso in item)]

    # Ritorna la lista dei file xml all'interno della directory
    def getListOfFiles(self, dirName, ext):
        listOfFile = os.listdir(dirName)
        allFiles = list()
        # Iterate over all the entries
        for entry in listOfFile:
            # Create full path
            fullPath = os.path.join(dirName, entry)
            # If entry is a directory then get the list of files in this directory
            if os.path.isdir(fullPath):
                allFiles = allFiles + self.getListOfFiles(fullPath, ext)
            else:
                if (os.path.splitext(fullPath)[1] == ext):
                    allFiles.append(fullPath)
        return allFiles

    def getListOfFilesFilterByAnno(self, dirName, ext1, ext2, mese, giorno, codiceFlusso):
        
        mese_giorno = ""
        allFiles = list()
        
        #return allFiles
        # Anno
        for anno in os.listdir(dirName):
            path_anno = os.path.join(dirName, anno)
            listOfFile = os.listdir(path_anno)

            for entry in listOfFile:
                        
                _mese = entry[0:2]
                _giorno = entry[2:4]
                
                
                if (mese != None and mese != '' and _mese != mese):
                        continue

                if (giorno != None and giorno != '' and _giorno != giorno):
                        continue


                fullPathAnno = os.path.join(dirName, anno, _mese+_giorno)
                if (os.path.isdir(fullPathAnno)):
                    for file_entry in os.listdir(fullPathAnno):
                        ext = os.path.splitext(file_entry)[1]
                        if (ext == ext1 or ext == ext2):
                            file_append = os.path.join(fullPathAnno, file_entry)
                            if (codiceFlusso != None and codiceFlusso != ''):
                                items = file_entry.split("_")
                                _codiceFlusso = items[3][0:3]
                                if (_codiceFlusso == codiceFlusso):
                                        allFiles.append(file_append)
                            else:
                                allFiles.append(file_append)

        return allFiles

    def getListOfFilesFilter2(self, dirName, ext1, ext2, anno, mese, giorno, codiceFlusso):
        listOfFile = os.listdir(dirName)
        mese_giorno = ""
        allFiles = list()
        try:
            if (anno != None):
                listOfFile = os.listdir(os.path.join(dirName, anno))
            else:
                return self.getListOfFilesFilterByAnno(dirName, ext1, ext2, mese, giorno, codiceFlusso)
        except:
                return list()
        
        
        #return allFiles
        # Anno
        for entry in listOfFile:
                
            _mese = entry[0:2]
            _giorno = entry[2:4]
            
            
            if (mese != None and mese != '' and _mese != mese):
                    continue

            if (giorno != None and giorno != '' and _giorno != giorno):
                    continue


            fullPathAnno = os.path.join(dirName, anno, _mese+_giorno)
            if (os.path.isdir(fullPathAnno)):
                for file_entry in os.listdir(fullPathAnno):
                   ext = os.path.splitext(file_entry)[1]
                   if (ext == ext1 or ext == ext2):

                        file_append = os.path.join(fullPathAnno, file_entry)
                        if (codiceFlusso != None and codiceFlusso != ''):
                            items = file_entry.split("_")
                            _codiceFlusso = items[3][0:3]
                            if (_codiceFlusso == codiceFlusso):
                                allFiles.append(file_append)
                        else:
                            allFiles.append(file_append)

        return allFiles
        

    def getListOfFilesFilter(self, dirName, ext, anno, mese, giorno, codiceFlusso):
        listOfFile = os.listdir(dirName)
        allFiles = list()
        # Iterate over all the entries
        for entry in listOfFile:
            # Create full path
            fullPath = os.path.join(dirName, entry)
            # If entry is a directory then get the list of files in this directory
            #print("FullPath: ", fullPath)
            if os.path.isdir(fullPath):
                allFiles = allFiles + self.getListOfFilesFilter(fullPath, ext, anno, mese, giorno, codiceFlusso)
            else:
                if (os.path.splitext(fullPath)[1] == ext):
                    if (self.filter2(entry, anno, mese, giorno, codiceFlusso,fullPath)):
                            #print("Append file:", fullPath)
                            allFiles.append(fullPath)
        return allFiles


    def filter2(self, nameFile, anno, mese, giorno, codiceFlusso, fullPath):
        result = True	
        print(fullPath)

        if (anno == None and anno == '' and \
            mese == None and mese == '' and \
            giorno == None and giorno == '' and \
            codiceFlusso == None and codiceFlusso == ''): 

                return True
        #13476050151_02971930165_201812_TML0050_20190109132853_1.xml.zip
        try:
                #print("Name file:", nameFile)
                items = nameFile.split("_")
                piva_distr = items[0]
                piva_utente = items[1]
                flusso = items[3]
                timestamp = items[4]

                #print(piva_distr, piva_utente, flusso, timestamp)

        except IndexError:
                print("IndexError:", nameFile)
                return False

        if (anno != None and anno != ''):
                anno_str = timestamp[:4]
                #print(timestamp, anno_str)
                result = result and anno_str == anno

        if (mese != None and mese != ''):
                mese_str = timestamp[4:6]
                result = result and mese_str == mese

        if (giorno != None and giorno != ''):
                giorno_str = timestamp[6:8]
                result = result and giorno_str == giorno
        
        if (codiceFlusso != None and codiceFlusso != ''):
                codiceFlusso_str = flusso[:3]
                result = result and codiceFlusso_str == codiceFlusso

        return result

    def filter(self, nameFile, anno, mese, giorno, codiceFlusso):
        result = True	


        if (anno == None and anno == '' and \
            mese == None and mese == '' and \
            giorno == None and giorno == '' and \
            codiceFlusso == None and codiceFlusso == ''): 

                return True
        #13476050151_02971930165_201812_TML0050_20190109132853_1.xml.zip
        try:
                #print("Name file:", nameFile)
                items = nameFile.split("_")
                piva_distr = items[0]
                piva_utente = items[1]
                flusso = items[3]
                timestamp = items[4]

                #print(piva_distr, piva_utente, flusso, timestamp)

        except IndexError:
                print("IndexError:", nameFile)
                return False

        if (anno != None and anno != ''):
                anno_str = timestamp[:4]
                #print(timestamp, anno_str)
                result = result and anno_str == anno

        if (mese != None and mese != ''):
                mese_str = timestamp[4:6]
                result = result and mese_str == mese

        if (giorno != None and giorno != ''):
                giorno_str = timestamp[6:8]
                result = result and giorno_str == giorno
        
        if (codiceFlusso != None and codiceFlusso != ''):
                codiceFlusso_str = flusso[:3]
                result = result and codiceFlusso_str == codiceFlusso

        return result
