# Job 1
#
# Requisito:
#
# Job di Acquisizione e Caricamento dei file relativi alla Dichiarazione Sostitutiva provenienti dal SIICloud 
# sotto la root TSG1_PivaUdD/Anno/mese
#
# Riferimento/Nota:
#
# Si richiede una procedura che a partire dalla piattaforma SIICloud effettui l'operazione di ingestione dei 
# file csv (zippati) provenienti dalla piattaforma SIICloud. Tale procedura a valle dell'ingestione deve 
# effettuare il caricamento su tabelle HIVE. I file avranno la seguente nomenclatura: 
# <PIVA_UdD>_TDS _ <AAAAMM>_<progressivo>.csv
# Se durante il caricamento si verificano anomalie dovute all'errata compilazione del file 
# (es: file vuoto, righe non formattate correttamente,ecc...) le anomalie devono essere tracciate in un apposito 
# archivio "ANOMALIE_FILE_DS".
#
#

import os
import zipfile
import re

class Job01:
    def valida(self, fileName):
        """ Procedura per validare il file.
        	Verifica della nomenclatura del file: <PIVA_UdD>_TDS_<AAAAMM>_<progressivo>.csv
        
        	Ritorna True se il file e' valido
        """
        
        is_valid = self.validate_filename(fileName)
        #TODO Altri controlli di validazione ???			
        	
        return is_valid

    def validate_filename(self, s):
    	return bool(re.match("\d{11,}_TDS_\d{6,}_\d*.\w{3,}$", s))
    
    def decomprimi(self, fileName, pathFile, dirDest):
        """ Decomprime il file zip.
        Ritorna True se la decompressione e' stata eseguita altrimenti False 
        """
        
        fileSrc = os.path.join( pathFile , fileName)
        
        try:    
            file_obj = zipfile.ZipFile(fileSrc, "r")
            file_obj.extractall(dirDest)
        except:
            return False
        
        return True
    
    def read_data(self, fileName, pathFile):
        """ Lettura del file name 
        """
    
    
    def save_data(self, sqlCtx, rdd, nameTableHive, schemaTableHive, cmdTableRefresh = "", partitionTableHive = None ):
        """ Procedura usata per savare i dati nella tabella HIVE """
        dataFrame = sqlCtx.createDataFrame(rdd, schema=schemaTableHive)
        
        if (partitionTableHive != None):
        	dataFrame.write.partitionBy(partitionTableHive).parquet(nameTableHive, 'append')
        else:
        	dataFrame.write.parquet(nameTableHive, 'append')
        
        sqlCtx.sql(cmdTableRefresh)
