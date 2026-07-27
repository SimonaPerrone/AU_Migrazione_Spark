# Release

20190625 
- Inserite chiamate funzioni Add Audit alla fine di ogni API
- Audit Elettrico/Gas spid-code: verrà inseriti nelle collection dei documenti contenenti lo spid-code passato nella request
- Aggiungi misure mancanti (***add_misure_mancanti***): Data la lista delle misure aggiunge le misure mancanti (buchi temporali) con valori NULL;
- Popola consumi (***popola_consumi***): calcola il consumo del giorno/mese precedente; recupera il consumo della data precedente e ne calcola il consumo come differenza della lettura attuale meno quella precedente.
- Fix Misure 1Gennaio 2019 (***fix_misure_1Gennaio2019***): Impostare il consumo giornaliero a null per la data piu recente del 1 gennaio 2019; 
- Remap dei campi Gas - Elettrico (***remap_fields_gas***, ***remap_fields_el***)
- Converte misure da string in float (***convert_misure***)

20190626
- Lettura Storico Gas (***get_csv_gas*** - /api/GetStoricoLettureGas)
- Lettura Storico Elettrico (query non valida) (***get_csv_el*** - /api/GetStoricoLettureElettriche)

20190627 
- Ordinata lista delle misure in output (***ordina_misure***)

20190603
- Inseriti nuove string per il remap "tipo_fornitura":
- "01": "default", 
- "02": "servizio di fornitura ultima istanza", 
- "03": "servizio di default"

20190604
- Modifica remap "tipo_fornitura": modificata la descrizione del codice "01" in "mercato libero o servizio di tutela"