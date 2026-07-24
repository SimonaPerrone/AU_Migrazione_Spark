import xml.etree.ElementTree as ET
import os
import re
from flussi.flussoTAL import FlussoTAL
from flussi.flussoSW1 import FlussoSW1
from flussi.flussoTGL import FlussoTGL
from flussi.flussoRML import FlussoRML
from flussi.flussoTML import FlussoTML
from flussi.flussoRGL import FlussoRGL
from flussi.flussoRSL import FlussoRSL
from flussi.flussoTAV import FlussoTAV
from flussi.flussoTAS import FlussoTAS
from flussi.flussoRMV import FlussoRMV
from flussi.flussoFUI import FlussoFUI
from flussi.flussoDEF import FlussoDEF
from flussi.flussoTMV import FlussoTMV
from flussi.flussoIM1 import FlussoIM1

import constants

class Data:
    def __init__(self, root, file_xml):
        self.root = root
        self.file_xml = file_xml
        

    @staticmethod
    def getFlusso(cod_servizio, cod_flusso):

        #flusso = FlussoTAL(cod_flusso)
        flusso = None
        
        if (cod_servizio == constants.CODSERVIZIO_SW1):     flusso = FlussoSW1(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_TGL):   flusso = FlussoTGL(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_RML):   flusso = FlussoRML(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_TML):   flusso = FlussoTML(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_RGL):   flusso = FlussoRGL(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_RSL):   flusso = FlussoRSL(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_TAL):   flusso = FlussoTAL(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_TAV):   flusso = FlussoTAV(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_TAS):   flusso = FlussoTAS(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_RMV):   flusso = FlussoRMV(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_FUI):   flusso = FlussoFUI(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_DEF):   flusso = FlussoDEF(cod_flusso)
        elif (cod_servizio == constants.CODSERVIZIO_TMV):   flusso = FlussoTMV(cod_flusso)
	elif (cod_servizio == constants.CODSERVIZIO_IM1):   flusso = FlussoIM1(cod_flusso)

        return flusso

    def toList(self, dataElaborazione):
        cod_servizio = self.root.get(constants.CODSERVIZIO_STR)
        cod_flusso = self.root.get(constants.CODFLUSSO_STR)

        flusso = Data.getFlusso(cod_servizio, cod_flusso)
        print("flusso:{}, local_file:{}".format(cod_servizio, self.file_xml))

        if (flusso == None):
            #print ("Flusso None")
            result  = [] 
            result.append(("","", dataElaborazione)) 
            return result

        if not cod_servizio in self.file_xml:
            print("--- Errore")
            #result = []
            #result.append(("","", dataElaborazione))
            #return result
        
        local_file = self.file_xml.replace("file:","")
        try:
            #print(self.file_xml)
            file_basename = os.path.basename(local_file)
            #print("file_basename: ", file_basename)
        
            file_items = file_basename.split("_")
            #print("file_items: ", file_items)

            if bool(re.match("^\d{4}$", file_items[2])):
                mese = file_items[2][:2]
                anno = "20" + file_items[2][2:4]
                #print("1. anno, mese: ", anno, mese)
            else:
                anno = file_items[2][:4]
                mese = file_items[2][4:6]
                #print("2. anno, mese: ", anno, mese)

	    if cod_servizio != "IM1":
                if not anno.isdigit() or not mese.isdigit():
	            print ("******** anno o mese non digit",anno, mese) 
                    return flusso.makeError(dataElaborazione = dataElaborazione, anno = "EE", mese = "EE", local_file = local_file)
            else:
	        anno = ""
		mese = ""

            if not flusso.isValidFile(file_basename) and cod_servizio != "IM1":
		print ("flusso non valido", local_file)
                return flusso.makeError(dataElaborazione = dataElaborazione, anno = anno, mese = mese, local_file = local_file)

            #print("(root, dataElaborazione, anno, mese): ", self.root, dataElaborazione, anno, mese)
	    print("getItems {}".format(local_file))
            result = flusso.getItems(self.root, dataElaborazione = dataElaborazione, anno = anno, mese = mese, local_file = local_file)
            #print("result:{}".format(result))
            #if(os.path.isfile(local_file)):
            #    os.rename(local_file, local_file+".chk")

            return result
	
        except Exception,e:
            print("Error: File:", local_file, str(e))
           
        anno = "0000"
        mese = "01"

        #print("(root, dataElaborazione, anno, mese): ", self.root, dataElaborazione, anno, mese)
        
	#print("local_file:{}".format(local_file))
        return flusso.makeError(dataElaborazione = dataElaborazione, anno = "EE", mese = "EE", local_file = local_file)
