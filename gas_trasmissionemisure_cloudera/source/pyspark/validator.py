import xmlschema
import os

class Validator:

    # Funzione per la validazione del flusso XML in ingresso
    def validateFlusso(self, flussoXML, file_xsd, file_xml):
	
        my_schema = xmlschema.XMLSchema(file_xsd)
        result = my_schema.is_valid(flussoXML)
	filename = os.path.basename(file_xml.replace("file:/",""))

	#print ("Validazione:", result,file_xml, file_xsd)
        # Validazione tramite xsd 
        if (result == False):
            return "004","I campi obbligatori non sono stati compilati o non sono stati correttamente compilati", False

        piva_distr  = flussoXML.find("IdentificativiRichiesta/piva_distr").text
        piva_utente = flussoXML.find("IdentificativiRichiesta/piva_utente").text
        codice_servizio = flussoXML.get("cod_servizio")
        cod_flusso      = flussoXML.get("cod_flusso")

        # controllo se il file xml e' valido
        fileXMLSplit = os.path.basename(file_xml).split("_")


        if not cod_flusso or not codice_servizio:
           return "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo", False

        isValidFile002 = True

	if codice_servizio != "IM1":
	    isValidFile002 = cod_flusso in file_xml
        if (isValidFile002 == False):
            return "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo (2)", False



        isValidFile001 = os.path.splitext(filename)[1].upper() == ".XML" or os.path.splitext(filename)[1].upper() == ".ZIP"
        if (isValidFile001 == False):
            return "001", "Il template (formato file e/o tracciato) utilizzato non e' congruo", False

        try:
            isValidFile003 = len((filename.split("_")[3] if len(filename.split("_")[3]) <= 3 else filename.split("_")[3][0:3])) == 3
            if (isValidFile003 == False):
                return "003", "Il codice univoco della prestazione non e' prevista", False
        except:
            return "003", "Il codice univoco della prestazione non e' prevista", False

        isValidFile908 = fileXMLSplit[0] == piva_distr and \
                         fileXMLSplit[1] == piva_utente 

        # Errore validazione 
        if (isValidFile908 == False):
            return "908", "L'identificato del richiedente non e' riconosciuto", False
     
        return "","",result and isValidFile908
