# Sintesi Funzionale Processi - AU_Migrazione_Spark

## Regole Applicate
- Usa solo informazioni presenti nel codice, README, `pom.xml`, driver, script di deploy e file di configurazione;
- Non inventare step non supportati dai file;
- Descrivi gli step a livello funzionale, non tecnico;
- Formato tabellare con colonne: `Progetto | Step | Descrizione`;
- Step numerati in ordine di esecuzione;
- Se il progetto ha più flow o varianti, sintetizzali nello stesso schema;
- Se mancano informazioni, segnala `non verificabile dai file`.

---

## Analisi Progetti

### 1. ingestion-misure-gas-unico

| Progetto | Step | Descrizione |
|---|---|---|
| ingestion-misure-gas-unico | 1. lettura input | legge i file ZIP di misura in ingresso |
| ingestion-misure-gas-unico | 2. decompressione | estrae i contenuti degli archivi compressi |
| ingestion-misure-gas-unico | 3. salvataggio intermedio | scrive i file decompressi in una cartella temporanea |
| ingestion-misure-gas-unico | 4. lettura elaborazione | rilegge i file estratti per l'analisi Spark |
| ingestion-misure-gas-unico | 5. verifica ammissibilità | applica controlli su struttura, naming, contenuto e regole business |
| ingestion-misure-gas-unico | 6. report esiti | genera file di report/ammissibilità per i file processati |
| ingestion-misure-gas-unico | 7. persistenza finale | salva gli esiti e i dati validi su Hive/Parquet |

---

### 2. gsv-gasivori

| Progetto | Step | Descrizione |
|---|---|---|
| gsv-gasivori | 1. lettura input | legge i dati CA (consumi prelievo) da Hive |
| gsv-gasivori | 2. preparazione dati | normalizza colonne e metadati |
| gsv-gasivori | 3. join business | unisce richieste e forniture |
| gsv-gasivori | 4. split per tipologia | separa ordinarie/suppletive/consuntive |
| gsv-gasivori | 5. aggregazione | calcola aggregati per ciascun ramo |
| gsv-gasivori | 6. union finale | combina i dataset dei rami con `unionByName` |
| gsv-gasivori | 7. scrittura output | persiste su Hive con cast tecnici |

---

### 3. pubblicazione_pcg

| Progetto | Step | Descrizione |
|---|---|---|
| pubblicazione_pcg | 1. lettura partizioni | seleziona la partizione corretta per `annomese`/`executionid` |
| pubblicazione_pcg | 2. trasformazione schema | mappa e rinomina colonne verso schema SBG misure |
| pubblicazione_pcg | 3. pivot giornaliero | applica pivot su `PRELIEVO_GIORN_*` e aggiunge colonne metadati |
| pubblicazione_pcg | 4. filtraggio record | filtra i record validi (`trattamento = G`) |
| pubblicazione_pcg | 5. preparazione distribuzione | distribuisce i record per piva e contatore CSV |
| pubblicazione_pcg | 6. scrittura CSV | genera i file CSV nel path output isilon |
| pubblicazione_pcg | 7. scrittura log | scrive il log pubblicazione in parquet partizionato su HDFS |

---

### 4. gas_trasmissionemisure_cloudera

| Progetto | Step | Descrizione |
|---|---|---|
| gas_trasmissionemisure_cloudera | 1. lettura query | esegue query SQL e ottiene lista file |
| gas_trasmissionemisure_cloudera | 2. filtraggio flusso | filtra per flusso desiderato |
| gas_trasmissionemisure_cloudera | 3. lettura file XML | legge contenuti file con `wholeTextFiles` |
| gas_trasmissionemisure_cloudera | 4. validazione nomenclatura | valida naming file |
| gas_trasmissionemisure_cloudera | 5. validazione XSD | valida tracciato XML |
| gas_trasmissionemisure_cloudera | 6. parsing per flusso | estrae e trasforma record per flusso |
| gas_trasmissionemisure_cloudera | 7. separazione risultati | divide record validi da non validi |
| gas_trasmissionemisure_cloudera | 8. scrittura output | scrive parquet partizionato e lancio `MSCK REPAIR TABLE` |

---

### 5. delete-old-partition

| Progetto | Step | Descrizione |
|---|---|---|
| delete-old-partition | 1. lettura configurazione | legge lista tabelle e retention da properties |
| delete-old-partition | 2. query partizioni | esegue `SHOW PARTITIONS` per ogni tabella |
| delete-old-partition | 3. lettura location | recupera path tabella con `DESC FORMATTED` |
| delete-old-partition | 4. filtraggio partizioni | filtra partizioni per sessione e data |
| delete-old-partition | 5. ordinamento | ordina per identificare quali rimuovere |
| delete-old-partition | 6. selezione surplus | mantiene `num.partition.remain`, seleziona surplus |
| delete-old-partition | 7. drop e cleanup | esegue `ALTER TABLE DROP PARTITION` e cancella path HDFS |

---

### 6. ee_aggregato_cloudera

| Progetto | Step | Descrizione |
|---|---|---|
| ee_aggregato_cloudera | 1. parsing CLI | riceve comando richiesto da `FlussoMisureTool` |
| ee_aggregato_cloudera | 2. dispatch comando | seleziona il modulo corrispondente (ingestione, aggregazione, SEM, portale, ecc.) |
| ee_aggregato_cloudera | 3. lettura input | legge dati Hive/HDFS specifici del comando |
| ee_aggregato_cloudera | 4. pulizia dati | applica filtri e normalizzazioni |
| ee_aggregato_cloudera | 5. elaborazione | esegue logica del comando selezionato |
| ee_aggregato_cloudera | 6. produzione output | genera dataset/file per il comando |
| ee_aggregato_cloudera | 7. scrittura risultati | persiste su tabelle/path target |

---

### 7. aggiustamento-bilanciamento-gas

| Progetto | Step | Descrizione |
|---|---|---|
| aggiustamento-bilanciamento-gas | non verificabile dai file | README generico senza dettagli implementazione |

---

### 8. indennizzi-misure-gas

| Progetto | Step | Descrizione |
|---|---|---|
| indennizzi-misure-gas | 1. lettura input ammissibilità | legge CSV TFC/VPG/QKRIUD e tabelle supporto |
| indennizzi-misure-gas | 2. validazione file | applica regole ammissibilità file |
| indennizzi-misure-gas | 3. validazione POD | applica regole ammissibilità POD |
| indennizzi-misure-gas | 4. produzione report ammissibilità | genera dataset esiti ammissibilità |
| indennizzi-misure-gas | 5. lettura input calcolo | legge tabelle TSG validate |
| indennizzi-misure-gas | 6. calcolo profili | esegue calcolo profili settlement |
| indennizzi-misure-gas | 7. scrittura calcolo | persiste output su tabelle ATG e tracking |

---

### 9. gse-energy-release

| Progetto | Step | Descrizione |
|---|---|---|
| gse-energy-release | 1. import Sqoop | importa perimetro/richieste da sorgente esterna |
| gse-energy-release | 2. lettura richieste | legge richieste da tabelle Hive |
| gse-energy-release | 3. preparazione consumi | normalizza dataset consumi |
| gse-energy-release | 4. join perimetro | unisce perimetro e richieste |
| gse-energy-release | 5. calcolo consumi | calcola aggregati energetici |
| gse-energy-release | 6. produzione risultati | genera dataset risultato finali |
| gse-energy-release | 7. scrittura output | persiste su Hive e export Sqoop |

---

### 10. partition-optimization

| Progetto | Step | Descrizione |
|---|---|---|
| partition-optimization | 1. lettura configurazione | carica tabelle e parametri da properties |
| partition-optimization | 2. lettura partizioni repartition | legge partizioni disponibili e crea backup |
| partition-optimization | 3. ottimizzazione | riscrive partizioni con numero file efficiente |
| partition-optimization | 4. check integrità repartition | verifica `exceptAll` per coerenza dati |
| partition-optimization | 5. lettura partizioni rollback | legge dati dal path `_backup` |
| partition-optimization | 6. ripristino | ripristina i dati dal backup su path originale |
| partition-optimization | 7. delete backup | elimina il path `_backup` dopo verifica esito positivo |

---

### 11. scambio-dati-gasivori

| Progetto | Step | Descrizione |
|---|---|---|
| scambio-dati-gasivori | 1. lettura parametri | legge mode richiesti (`CC`, `CSEA`, `ID`, `UDD`, `UDB`, `AMM`) |
| scambio-dati-gasivori | 2. validazione mode | verifica che i mode siano ammessi |
| scambio-dati-gasivori | 3. lettura input | legge tabelle input per mode scelti |
| scambio-dati-gasivori | 4. factory selezione | mappa mode a aggregatori specifici |
| scambio-dati-gasivori | 5. elaborazione aggregatori | esegue aggregatori in sequenza |
| scambio-dati-gasivori | 6. produzione output | genera dataset pubblicazione per ogni mode |
| scambio-dati-gasivori | 7. export esiti | esegue Sqoop export verso sistema esterno |

---

### 12. aggregatore_cdp

| Progetto | Step | Descrizione |
|---|---|---|
| aggregatore_cdp | 1. lettura CA final | carica dataset CA partizionato |
| aggregatore_cdp | 2. normalizzazione | applica trasformazioni e metadati |
| aggregatore_cdp | 3. factory aggregatori | seleziona aggregatori da `output.file.couples` |
| aggregatore_cdp | 4. produzione dataset | ogni aggregatore produce output specifico |
| aggregatore_cdp | 5. scrittura Hive | persiste output su tabelle Hive |
| aggregatore_cdp | 6. repair metadati | esegue `MSCK REPAIR TABLE` se previsto |
| aggregatore_cdp | 7. export Sqoop | esegue Sqoop export su Oracle quando previsto |

---

### 13. cce-calcolo

| Progetto | Step | Descrizione |
|---|---|---|
| cce-calcolo | 1. parsing argomenti | riceve `tipoCalc` in `{P, PR, Pein, PRein}` e data |
| cce-calcolo | 2. validazione | verifica obbligo `massivoFlag` per PR/PRein |
| cce-calcolo | 3. lettura anagrafiche | carica RCU/RCUS e perimetro |
| cce-calcolo | 4. lettura misure | legge flussi misure quarti/estensione |
| cce-calcolo | 5. check ammissibilità | unisce report pod/file e prodotti ammissibilità |
| cce-calcolo | 6. selezione branch | esegue calcoli specifici per `tipoCalc` |
| cce-calcolo | 7. produzione annullamenti | genera annullamenti `1g/2g` o `1gEin/2gEin` |
| cce-calcolo | 8. calcolo finali | esegue calcolo P/PR/Pein/PRein |
| cce-calcolo | 9. scrittura output | persiste risultati su tabelle Hive |

---

### 14. portale-consumi-common

| Progetto | Step | Descrizione |
|---|---|---|
| portale-consumi-common | 1. backup collezione | legge parametri backup da properties |
| portale-consumi-common | 2. connessione Mongo | accede alle collezioni Mongo |
| portale-consumi-common | 3. backup esecuzione | esegue `aggregate {$out: "<collection>_BKP"}` |
| portale-consumi-common | 4. pulizia audit | legge soglia mesi e data limite |
| portale-consumi-common | 5. filtraggio audit | seleziona record audit da rimuovere |
| portale-consumi-common | 6. esecuzione pulizia | esegue `$pull` su array audit con data `< soglia` |
| portale-consumi-common | 7. logging risultati | scrive log di backup/pulizia |

---

### 15. calcolo-capacita

| Progetto | Step | Descrizione |
|---|---|---|
| calcolo-capacita | 1. lettura parametri | riceve data calcolo, X, Y, verbosity |
| calcolo-capacita | 2. lettura input | carica misure, perimetri, anagrafiche |
| calcolo-capacita | 3. pulizia dati | applica filtri e normalizzazioni |
| calcolo-capacita | 4. elaborazione flow | esegue `CalcoloCtcFlow.run()` con logica di calcolo |
| calcolo-capacita | 5. produzione dataset | genera dataset risultato capacità |
| calcolo-capacita | 6. scrittura output | persiste risultati su Hive/HDFS |

---

### 16. pubblicazione_cce

| Progetto | Step | Descrizione |
|---|---|---|
| pubblicazione_cce | 1. lettura data | valida data richieste (default = giorno precedente) |
| pubblicazione_cce | 2. validazione flow | verifica flow ammesso |
| pubblicazione_cce | 3. lettura input | carica tabelle CCE calcolo/anagrafica/track |
| pubblicazione_cce | 4. lettura richieste | legge richieste Sqoop importate |
| pubblicazione_cce | 5. normalizzazione | normalizza richieste e esiti |
| pubblicazione_cce | 6. dispatch flow | seleziona flow `P`, `PR`, `PEIN`, `PREIN`, `CA` |
| pubblicazione_cce | 7. elaborazione | esegue logica specifica per flow |
| pubblicazione_cce | 8. scrittura esiti | persiste tabelle output pubblicazione |

---

### 17. ee_switching

| Progetto | Step | Descrizione |
|---|---|---|
| ee_switching | 1. parsing argomenti | valida combinazioni input (timestamp/liste/date) |
| ee_switching | 2. validazione combinazioni | controlla obblighi e incompatibilità |
| ee_switching | 3. lettura input | carica dataset switching da Hive |
| ee_switching | 4. lettura parametri | legge file parametrici HDFS se presenti |
| ee_switching | 5. pulizia dati | normalizza liste, date, timestamp |
| ee_switching | 6. applicazione filtri | applica perimetro e filtri run ordinaria/parametrica |
| ee_switching | 7. dispatch flow | seleziona `FunzionaliFlow` o `StoriciFlow` |
| ee_switching | 8. elaborazione | esegue transformazioni per finestra richiesta |
| ee_switching | 9. produzione output | genera dataset file per flussi/finestra |
| ee_switching | 10. scrittura output | persiste output e genera XML/ZIP |

---

### 18. trasmissione-settlement-gas

| Progetto | Step | Descrizione |
|---|---|---|
| trasmissione-settlement-gas | 1. lettura CSV ammissibilità | carica file TFC/VPG/QKRIUD |
| trasmissione-settlement-gas | 2. validazione file | applica regole filename/header/estensione/campi |
| trasmissione-settlement-gas | 3. produzione report ammissibilità | genera report esiti file |
| trasmissione-settlement-gas | 4. produzione TSG ammissibili | genera tabella TSG record ammessi |
| trasmissione-settlement-gas | 5. lettura TSG validati | carica tabelle TSG ammissibili |
| trasmissione-settlement-gas | 6. selezione latest record | seleziona record più recente per chiave |
| trasmissione-settlement-gas | 7. check cardinalità giorni | verifica giorni presenti nel mese |
| trasmissione-settlement-gas | 8. calcolo profili | esegue calcolo profili settlement |
| trasmissione-settlement-gas | 9. gestione backup | crea/controlla/elimina backup ATG |
| trasmissione-settlement-gas | 10. scrittura ATG | persiste su tabelle ATG collegate |

---

### 19. freezer_pre_calcolo

| Progetto | Step | Descrizione |
|---|---|---|
| freezer_pre_calcolo | 1. parsing argomenti | riceve sessione (`CDP/CCG_FIN/CCG_RIC`) e data opzionale |
| freezer_pre_calcolo | 2. validazione sessione | verifica sessione ammessa |
| freezer_pre_calcolo | 3. setup environment | imposta environment specifico per sessione |
| freezer_pre_calcolo | 4. lettura RCUGAS | carica tabelle RCUGAS richieste |
| freezer_pre_calcolo | 5. factory freezer | seleziona freezer da `input.table.freezer` |
| freezer_pre_calcolo | 6. elaborazione freezer | ogni freezer processa i propri dati |
| freezer_pre_calcolo | 7. produzione dataset congelato | genera dataset freeze per sessione/data |
| freezer_pre_calcolo | 8. scrittura freeze | persiste su tabelle freeze `rcugas_*_freeze` |

---

### 20. sgs-flusso-storico-gas

| Progetto | Step | Descrizione |
|---|---|---|
| sgs-flusso-storico-gas | 1. perimetro - lettura input | legge raw sqoop e tabelle RCUGAS |
| sgs-flusso-storico-gas | 2. perimetro - filtraggio | applica filtri giorno e flag attivazione |
| sgs-flusso-storico-gas | 3. perimetro - elaborazione | applica regole perimetro |
| sgs-flusso-storico-gas | 4. perimetro - scrittura | salva perimetri per prossima fase |
| sgs-flusso-storico-gas | 5. aggregazione - lettura | carica consumi/perimetri/anagrafiche |
| sgs-flusso-storico-gas | 6. aggregazione - calcoli | applica regole aggregazione (SWG/UIG/VTG) |
| sgs-flusso-storico-gas | 7. aggregazione - scrittura | salva aggregati e info tracking |
| sgs-flusso-storico-gas | 8. pubblicazione - lettura | carica aggregati validati |
| sgs-flusso-storico-gas | 9. pubblicazione - regole | applica regole pubblicazione |
| sgs-flusso-storico-gas | 10. pubblicazione - scrittura | persiste output finale e report |

---

### 21. ccg-pubblicazione

| Progetto | Step | Descrizione |
|---|---|---|
| ccg-pubblicazione | 1. parsing parametri | valida flow e data (default = giorno precedente) |
| ccg-pubblicazione | 2. validazione sessione | verifica sessione in {AGG, SBG, CDP_FIN, CDP_RIC} |
| ccg-pubblicazione | 3. import Sqoop | esegue import richieste per sessione |
| ccg-pubblicazione | 4. lettura input | carica dati e richieste per sessione |
| ccg-pubblicazione | 5. normalizzazione | normalizza valori e sessione |
| ccg-pubblicazione | 6. dispatch sessione | seleziona flow per sessione |
| ccg-pubblicazione | 7. elaborazione | esecuzione logica sessione-specifica |
| ccg-pubblicazione | 8. scrittura output | persiste risultati su tabelle output |
| ccg-pubblicazione | 9. export esiti | esegue export Sqoop |

---

### 22. portale-consumi-ee

| Progetto | Step | Descrizione |
|---|---|---|
| portale-consumi-ee | 1. parsing flow | riceve nome flow e flag storico |
| portale-consumi-ee | 2. lettura input | carica sorgenti Hive e Mongo (se previsto) |
| portale-consumi-ee | 3. filtraggio temporale | applica filtri temporali per finestra |
| portale-consumi-ee | 4. normalizzazione | normalizza record per schema target |
| portale-consumi-ee | 5. elaborazione flow | esegue logica specifica flow |
| portale-consumi-ee | 6. produzione dataset | genera dataset output per flow |
| portale-consumi-ee | 7. scrittura Hive | persiste su Hive tabelle target |
| portale-consumi-ee | 8. scrittura Mongo | sincronizza su collection Mongo (se previsto) |

---

### 23. aggiustamento-gas

| Progetto | Step | Descrizione |
|---|---|---|
| aggiustamento-gas | 1. parsing sessione | legge sessione (`AGG/CCG/SBG`) e data |
| aggiustamento-gas | 2. lettura misure | carica misure e tabelle RCUGAS numerose |
| aggiustamento-gas | 3. filtraggio inclusione/esclusione | applica regole di selezione |
| aggiustamento-gas | 4. annullamenti | applica annullamenti e rettifiche |
| aggiustamento-gas | 5. dedupliche | applica dedupliche e priorità |
| aggiustamento-gas | 6. calcolo trattamenti | calcola trattamento, priorità, join anagrafica |
| aggiustamento-gas | 7. calcolo coefficienti | applica coefficienti e segmenti |
| aggiustamento-gas | 8. consumi giornalieri | calcola consumi giornalieri |
| aggiustamento-gas | 9. controlli esclusioni | applica esclusioni forzate e incoerenze |
| aggiustamento-gas | 10. scrittura output | persiste validated_flow, consumi, esclusi, incoerenti |

---

### 24. cagas

| Progetto | Step | Descrizione |
|---|---|---|
| cagas | 1. parsing sessione | riceve sessione (`CDP/CCG_FIN/CCG_RIC`) e data opzionale |
| cagas | 2. lettura input | carica misure, anagrafica RCU/RCUGAS, lookup climatici |
| cagas | 3. filtraggio lancio | applica filtri lancio e forzature |
| cagas | 4. selezione ultima versione | seleziona ultima versione misure |
| cagas | 5. validazione misure | esegue fase VALIDAZIONE |
| cagas | 6. calcolo consumi | esegue fase CONSUMI |
| cagas | 7. calcolo CA | esegue fase CA |
| cagas | 8. CA pre-final | esegue fase CA_PRE_FINAL |
| cagas | 9. CA final | esegue fase CA_FINAL con enrichment |
| cagas | 10. scrittura output | persiste tabelle validation/consumption/ca/final |

---

### 25. meccanismo-incentivante-distributori-gas

| Progetto | Step | Descrizione |
|---|---|---|
| meccanismo-incentivante-distributori-gas | 1. parsing flow | riceve flow richiesto |
| meccanismo-incentivante-distributori-gas | 2. validazione flow | verifica flow tra quelli supportati |
| meccanismo-incentivante-distributori-gas | 3. lettura input | carica dati per flow scelto |
| meccanismo-incentivante-distributori-gas | 4. pulizia dati | applica controlli e normalizzazioni |
| meccanismo-incentivante-distributori-gas | 5. selezione branch | dispatch su flow (SBG/AGG/AGG_BIT/MID1_PREP/MID1_PUBB/MID2_PREP/MID2_PUBB) |
| meccanismo-incentivante-distributori-gas | 6. elaborazione | esecuzione logica branch |
| meccanismo-incentivante-distributori-gas | 7. produzione output | creazione dataset/file risultato |
| meccanismo-incentivante-distributori-gas | 8. scrittura output | persistenza su tabelle/file zip |

---

### 26. aggregatore-consumi-agg

| Progetto | Step | Descrizione |
|---|---|---|
| aggregatore-consumi-agg | 1. lettura daily_consumption | carica dataset consumi giornalieri |
| aggregatore-consumi-agg | 2. lettura incoerenti | carica record incoerenti |
| aggregatore-consumi-agg | 3. lettura esclusi | carica record esclusi |
| aggregatore-consumi-agg | 4. join chiavi | unisce su chiavi (pdr, date, session, executionid) |
| aggregatore-consumi-agg | 5. valorizzazione campi | applica sterilizzazione e normalizzazione value |
| aggregatore-consumi-agg | 6. factory aggregatori | seleziona aggregatori da property |
| aggregatore-consumi-agg | 7. applicazione coefficienti | applica `CoefficientController` se previsto |
| aggregatore-consumi-agg | 8. produzione aggregati | costruisce dataset per processi AGG/SBG |
| aggregatore-consumi-agg | 9. scrittura output | persiste su tabelle output e repair metadata |

---

### 27. cdp-codprofstd-tds

| Progetto | Step | Descrizione |
|---|---|---|
| cdp-codprofstd-tds | 1. parsing argomenti | riceve properties path |
| cdp-codprofstd-tds | 2. lettura config | carica proprietà da file |
| cdp-codprofstd-tds | 3. logging execution id | stampa identificativo run e data partizione |
| cdp-codprofstd-tds | 4. lettura input | carica dati TDS |
| cdp-codprofstd-tds | 5. normalizzazione | normalizza campi TDS |
| cdp-codprofstd-tds | 6. ricalcolo | esegue controller `TdsCodProfStd.run()` |
| cdp-codprofstd-tds | 7. scrittura output | persiste codprofstd ricalcolato su tabelle target |

---

### 28. erp-e-profilazione-punti-no

| Progetto | Step | Descrizione |
|---|---|---|
| erp-e-profilazione-punti-no | 1. parsing argomenti | valida flow e path pod esclusi |
| erp-e-profilazione-punti-no | 2. validazione flow | verifica flow in lista ammessa |
| erp-e-profilazione-punti-no | 3. lettura input | carica dataset ERP |
| erp-e-profilazione-punti-no | 4. filtraggio pod esclusi | applica esclusioni da file pod |
| erp-e-profilazione-punti-no | 5. normalizzazione | normalizza input |
| erp-e-profilazione-punti-no | 6. dispatch flow | seleziona flow (DIST/TERNA/IP/INT/FULL/NO/SALDO/DATI) |
| erp-e-profilazione-punti-no | 7. elaborazione flow | esecuzione logica specifica flow |
| erp-e-profilazione-punti-no | 8. produzione output | creazione dataset risultato |
| erp-e-profilazione-punti-no | 9. scrittura output | persistenza su tabelle/path previsti |

---

### 29. ingestion-elettrico-ammissibilita

| Progetto | Step | Descrizione |
|---|---|---|
| ingestion-elettrico-ammissibilita | 1. pre-check Sqoop | verifica aggiornamento tabelle RCU/RCUGAS |
| ingestion-elettrico-ammissibilita | 2. parsing opzioni | riceve tipo flusso (1G/2G/SMIS) e finestra giorno |
| ingestion-elettrico-ammissibilita | 3. lettura cartelle XML | accede ai path dei file da validare |
| ingestion-elettrico-ammissibilita | 4. selezione file validi | filtra per naming e pattern |
| ingestion-elettrico-ammissibilita | 5. normalizzazione metadata | standardizza metadati estratti |
| ingestion-elettrico-ammissibilita | 6. validazione file | applica regole ammissibilità file |
| ingestion-elettrico-ammissibilita | 7. validazione POD | applica regole ammissibilità POD |
| ingestion-elettrico-ammissibilita | 8. produzione report | genera report esiti file e POD |
| ingestion-elettrico-ammissibilita | 9. scrittura log | persiste report e log su Hive/path output |

---

### 30. portale-consumi

| Progetto | Step | Descrizione |
|---|---|---|
| portale-consumi | 1. CLI dispatcher | riceve comando da `FlussoMisureTool` |
| portale-consumi | 2. parsing CLI | estrae e valida opzioni del comando |
| portale-consumi | 3. selezione comando | instrada al comando selezionato |
| portale-consumi | 4. lettura input | carica sorgenti per comando |
| portale-consumi | 5. pulizia dati | applica filtri e normalizzazioni comando-specifiche |
| portale-consumi | 6. elaborazione | esecuzione logica comando (injection/ammissibilita/

---

### 30. portale-consumi

| Progetto | Step | Descrizione |
|---|---|---|
| portale-consumi | 1. CLI dispatcher | riceve comando da `FlussoMisureTool` |
| portale-consumi | 2. parsing CLI | estrae e valida opzioni del comando |
| portale-consumi | 3. selezione comando | instrada al comando selezionato |
| portale-consumi | 4. lettura input | carica sorgenti per comando |
| portale-consumi | 5. pulizia dati | applica filtri e normalizzazioni comando-specifiche |
| portale-consumi | 6. elaborazione | esecuzione logica comando (injection/ammissibilita/aggregazione/pubblicazione) |
| portale-consumi | 7. scrittura output | persiste su Hive/Mongo/file zip per portale |
| portale-consumi | 8. gestione audit | traccia audit log su Mongo |

---

### 31. portale-consumi-2.0

| Progetto | Step | Descrizione |
|---|---|---|
| portale-consumi-2.0 | 1. parsing flow | riceve flow e opzioni per versione 2.0 |
| portale-consumi-2.0 | 2. lettura configurazione | carica proprietà versione 2.0 |
| portale-consumi-2.0 | 3. lettura sorgenti | carica dataset Hive/Mongo per flow |
| portale-consumi-2.0 | 4. trasformazione schema | mappa verso schema portale 2.0 |
| portale-consumi-2.0 | 5. arricchimento dati | aggiunge metadati e campi calcolati |
| portale-consumi-2.0 | 6. filtraggio temporale | applica finestre richieste |
| portale-consumi-2.0 | 7. aggregazione | esecuzione aggregatori per flow |
| portale-consumi-2.0 | 8. validazione | controlla coerenza e completezza |
| portale-consumi-2.0 | 9. scrittura Hive/Mongo | persiste output partizionato/collezione |

---

### 32. sbg-sessione-bilanciamento-gas

| Progetto | Step | Descrizione |
|---|---|---|
| sbg-sessione-bilanciamento-gas | 1. parsing sessione | riceve data e opzioni sessione |
| sbg-sessione-bilanciamento-gas | 2. validazione data | verifica data ammessa e vincoli finestra |
| sbg-sessione-bilanciamento-gas | 3. check prerequisiti | valida completamento calcoli pre-SBG |
| sbg-sessione-bilanciamento-gas | 4. lettura input | carica tabelle misure, CA, profili da sessioni precedenti |
| sbg-sessione-bilanciamento-gas | 5. factory flow | seleziona flow SBG (AGG_BIT/MID1/MID2/PUBB/ECC) |
| sbg-sessione-bilanciamento-gas | 6. elaborazione bilanciamento | esecuzione calcoli SBG per flow |
| sbg-sessione-bilanciamento-gas | 7. produzione risultati | generazione dataset SBG risultato |
| sbg-sessione-bilanciamento-gas | 8. scrittura tabelle SBG | persiste su tabelle output SBG partizionate |
| sbg-sessione-bilanciamento-gas | 9. repair metadata | esegue `MSCK REPAIR TABLE` |
| sbg-sessione-bilanciamento-gas | 10. export Sqoop | sincronizza output verso sistemi external |

---

## Riepilogo Complessivo

| # | Progetto | Fasi Principali | Complessità |
|---|---|---|---|
| 1 | ingestion-misure-gas-unico | Lettura → Decomp → Validazione → Report → Persistenza | Media |
| 2 | gsv-gasivori | Lettura → Join → Split → Aggregazione → Union | Media |
| 3 | pubblicazione_pcg | Partizione → Transform → Pivot → Filtraggio → CSV/Log | Alta |
| 4 | gas_trasmissionemisure_cloudera | Query → XML Parse → Validazione → Split → Repair | Alta |
| 5 | delete-old-partition | Config → Query → Filtraggio → Drop → Cleanup | Bassa |
| 6 | ee_aggregato_cloudera | Dispatch → Lettura → Pulizia → Elaborazione → Persistenza | Media |
| 7 | aggiustamento-bilanciamento-gas | Non verificabile dai file | ? |
| 8 | indennizzi-misure-gas | Validazione → Report → Calcolo → Persistenza | Alta |
| 9 | gse-energy-release | Import → Join → Calcolo → Export | Media |
| 10 | partition-optimization | Backup → Repartition → Verifica → Rollback | Media |
| 11 | scambio-dati-gasivori | Lettura mode → Factory → Elaborazione → Export | Media |
| 12 | aggregatore_cdp | Lettura → Factory aggregatori → Scritti Hive/Sqoop | Media |
| 13 | cce-calcolo | Parsing → Validazione → Lettura → Calcoli branch → Persistenza | Molto Alta |
| 14 | portale-consumi-common | Backup Mongo → Pulizia audit → Logging | Bassa |
| 15 | calcolo-capacita | Lettura → Pulizia → Elaborazione flow → Persistenza | Media |
| 16 | pubblicazione_cce | Dispatch flow → Lettura → Normalizzazione → Scrittura | Alta |
| 17 | ee_switching | Validazione → Lettura → Filtraggio → Dispatch flow → XML/ZIP | Molto Alta |
| 18 | trasmissione-settlement-gas | Validazione → Report → TSG → Calcolo profili → ATG | Molto Alta |
| 19 | freezer_pre_calcolo | Parsing → Factory freezer → Elaborazione → Persistenza | Media |
| 20 | sgs-flusso-storico-gas | Perimetro → Aggregazione → Pubblicazione (3 phase) | Molto Alta |
| 21 | ccg-pubblicazione | Parsing → Import Sqoop → Dispatch → Elaborazione → Export | Alta |
| 22 | portale-consumi-ee | Parsing flow → Lettura → Filtraggio → Elaborazione → Hive/Mongo | Media |
| 23 | aggiustamento-gas | Filtraggio → Annullamenti → Dedupliche → Calcoli → Esclusioni | Molto Alta |
| 24 | cagas | Parsing → Validazione → Consumi → CA → CA_FINAL → Persistenza | Molto Alta |
| 25 | meccanismo-incentivante-distributori-gas | Dispatch flow → Elaborazione branch → Output ZIP | Media |
| 26 | aggregatore-consumi-agg | Join → Normalizzazione → Factory aggregatori → Coefficienti | Media |
| 27 | cdp-codprofstd-tds | Lettura → Normalizzazione → Ricalcolo → Persistenza | Bassa |
| 28 | erp-e-profilazione-punti-no | Filtraggio → Dispatch flow → Elaborazione → Persistenza | Media |
| 29 | ingestion-elettrico-ammissibilita | File selection → Normalizzazione → Validazione file/POD → Report | Alta |
| 30 | portale-consumi | CLI dispatch → Elaborazione comando → Persistenza Hive/Mongo | Media |
| 31 | portale-consumi-2.0 | Lettura → Transform schema → Arricchimento → Aggregazione → Persistenza | Media |
| 32 | sbg-sessione-bilanciamento-gas | Validazione → Factory flow → Calcolo → Persistenza → Export | Molto Alta |

---

## Note Tecniche

### Progetti con Altissima Complessità
- **cce-calcolo, ee_switching, trasmissione-settlement-gas, sgs-flusso-storico-gas, aggiustamento-gas, cagas, sbg-sessione-bilanciamento-gas**: richiedono migrazione prioritaria e testing estensivo
- Causa: multi-step con decisioni branch, calcoli iterativi, persistenza multipla

### Progetti PySpark vs Scala
- **gas_trasmissionemisure_cloudera**: PySpark (verificare migration path Python 3 + Spark 3)
- Tutti gli altri: Scala 2.11 → 2.12 (standard migration path)

### Pattern Ricorrenti
1. **Read + Factory Pattern** (6, 11, 12, 19, 23, 26): selezione dinamica step per configurazione
2. **Multi-phase Pipeline** (20, 23, 24, 32): elaborazione in fasi con checkpoints intermedi
3. **Validation Report** (16, 18, 29): generazione esiti/ammissibilità preliminari
4. **Sqoop Integration** (9, 12, 21, 32): import/export verso database esterno

### Verificabilità Limitata
- **Progetto 7 (aggiustamento-bilanciamento-gas)**: README generico, dettagli driver non disponibili

---

## Istruzioni per l'Uso

1. **Salva questo file** come `0.TEMPLATE_Sintesi_funzionale_COMPLETO.md`
2. **Consulta per ogni progetto** i step funzionali esatti in ordine sequenziale
3. **Usalo durante migrazione Spark 2→3** per validare che ogni step sia stato adaptato ai nuovi constraint CDP 7.1.9
4. **Incrocia con le guide di migrazione** generate in precedenza per ogni progetto (stack, configurazione, test)

---

## Template per Aggiungere Nuovi Progetti

Se in futuro si aggiungono progetti al repository, usa questo template:

| Progetto | Step | Descrizione |
|---|---|---|
| `<nome_progetto>` | 1. `<step_1>` | `<descrizione_funzionale>` |
| `<nome_progetto>` | 2. `<step_2>` | `<descrizione_funzionale>` |
| `<nome_progetto>` | 3. `<step_3>` | `<descrizione_funzionale>` |
| ... | ... | ... |

---

**Generato:** sintesi funzionali da codice sorgente (Driver/Main, pom.xml, deploy scripts)  
**Data:** Come da richiesta utente  
**Repository:** AU_Migrazione_Spark - 32 progetti  
**Target:** Migrazione Spark 2 → Spark 3 su CDP 7.1.9
