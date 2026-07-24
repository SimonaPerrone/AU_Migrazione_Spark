
class UtilTime:

    # Conversione data/ora da Solare a Locale
    # value: valore da convertire
    # index: indice dell'ora
    # mese: numero del mese
    # dst: Dst attributo dell'elemento xml
    # kr_ka: attributi xml
    def convertSolareLocale(self, value, index, mese, dst, kr_ka=1):
        # TODO da definire
        # La conversione viene attivata quando il valore != None e i mese e' Ottobre (10)
        if (value != None and mese == 10):
            if (dst == "2"):
                if (index >= 1 and index <= 12):
                    return (float(value.replace(",", "."))) * kr_ka

                if (dst == "3"):
                    if (index >= 9 and index <= 12):
                        return (float(value.replace(",", "."))) * kr_ka

                if (index >= 13 and index <= 96):
                    return (float(value.replace(",", "."))) * kr_ka

                return (float(value.replace(",", "."))) * kr_ka

        else:
            #  Se il valore e' None oppure il mese non e' Ottobre (10)
            #  ritorna il valore senza nessuna modifica
            return value
