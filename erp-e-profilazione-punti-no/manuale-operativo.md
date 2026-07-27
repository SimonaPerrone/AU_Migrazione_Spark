# Manuale Operativo - ERP (Energia Residuale Parziale)

## Introduzione
Questo documento rappresenta il manuale operativo per l'utilizzo del software di calcolo e pubblicazione ERP (Energia Residuale Parziale). Per il dettaglio delle logiche applicative fare riferimento al documento di analisi, riferimento R01.

## Documenti di riferimento
| Codice | Titolo |
| --- | --- |
| R01 | Enermia Residuale Parziale.md |

## Esecuzione processo ERP
Si descrivono di seguito i passi necessari per eseguire il processo di calcolo ERP:

1. Accedere su Hue (ambiente di riferimento) e posizionarsi nel percorso HDFS di deploy per modificare i file di configurazione. Percorsi standard: `/user/eng_test/ERP/dev/deploy/conf` per collaudo, `/user/eng_test/ERP/pro/deploy/conf` per produzione.

2. Configurare i parametri di lancio modificando il file `job.properties` salvato in HDFS. Valorizzare i campi come da specifica:
   - `spark.app.energia_residuale_pariziale.paramentro.annomese=YYYYMM` (obbligatorio, formato anno/mese).
   - `spark.app.energia_residuale_pariziale.paramentro.area=<A|B|...>` (opzionale, vuoto per tutte le aree).
   - `spark.app.energia_residuale_pariziale.paramentro.singola_piva_distributore=<PIVA>` (opzionale, esegue il calcolo solo per quel distributore).
   - `spark.app.energia_residuale_pariziale.paramentro.piva_esclusa=05779661007` (PIVA Terna da escludere ove previsto).
   - Solo se si intende lanciare anche la pubblicazione nello stesso run impostare, secondo l'ambiente, i path di staging:  
     `spark.app.energia_residuale_pariziale.paramentro.dati_output_path=<percorso base output>`  
     `spark.app.energia_residuale_pariziale.paramentro.dati_output_mount=<sotto-cartella opzionale>`

   Predisporre inoltre il file `pod_esclusi.txt` (uno per riga) da passare all'opzione `--path_pod_esclusi`, ad esempio `/user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt`.

3. Lanciare il calcolo tramite shell sull'edge node (utenza applicativa). Portarsi nella cartella `${deploy.path.erp}` e utilizzare lo script `spark-submit-erp.sh` indicando il file properties in HDFS e il file dei POD esclusi. Esempi:
   - Lancio completo orchestrato (DIST + TERNA + IP + INT in parallelo, poi NO e SALDO):
     ```bash
     nohup bash ./spark-submit-erp.sh \
       --calcolo FULL \
       --properties /user/eng_test/ERP/dev/deploy/conf/job.properties \
       --path_pod_esclusi /user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt \
       2>&1 | tee ${deploy.path.erp}/logs/erp-full-$(date +%Y%m%d%H%M%S).log &
     ```
   - Lancio di un singolo flusso (DIST | TERNA | IP | INT | NO):
     ```bash
     nohup bash ./spark-submit-erp.sh \
       --calcolo NO \
       --properties /user/eng_test/ERP/dev/deploy/conf/job.properties \
       --path_pod_esclusi /user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt \
       2>&1 | tee ${deploy.path.erp}/logs/erp-no-$(date +%Y%m%d%H%M%S).log &
     ```
   - Lancio del solo SALDO (aggregazione finale dopo i calcoli):
     ```bash
     nohup bash ./spark-submit-erp.sh \
       --calcolo SALDO \
       --properties /user/eng_test/ERP/dev/deploy/conf/job.properties \
       --path_pod_esclusi /user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt \
       2>&1 | tee ${deploy.path.erp}/logs/erp-saldo-$(date +%Y%m%d%H%M%S).log &
     ```

   I log vengono redirezionati nella cartella `logs` del deploy locale; e' possibile usare `tail -f` sul file di log appena creato per monitorare l'avanzamento.

## Esecuzione processo Pubblicazione aggregati
Si descrivono i passi necessari per eseguire il processo di pubblicazione dei flussi ERP:

1. Accedere su Hue e modificare il file `job.properties` in HDFS impostando l'annomese di pubblicazione e i parametri specifici della messa a disposizione:
   - `spark.app.energia_residuale_pariziale.paramentro.annomese=YYYYMM` (obbligatorio).
   - `spark.app.energia_residuale_pariziale.paramentro.dati_output_path=<percorso base output>` (obbligatorio se non si usa l'opzione `--DATI`).
   - `spark.app.energia_residuale_pariziale.paramentro.dati_output_mount=<sotto-cartella opzionale>` (eventuale mount o subpath da concatenare all'output base).
   - `spark.app.energia_residuale_pariziale.paramentro.pub_tipo=<AC4|AC2|AC4_INT>` (AC4 default per distributori; AC2 per Terna; AC4_INT esegue solo lo ZIP di dettaglio POD Interconnessione).
   - `spark.app.energia_residuale_pariziale.paramentro.piva_terna=<PIVA>` (opzionale, default 05779661007).

2. Lanciare il processo dalla shell con l'utente applicativo:
   ```bash
   nohup bash ./spark-submit-erp.sh \
     --calcolo DATI \
     --properties /user/eng_test/ERP/dev/deploy/conf/job.properties \
     --path_pod_esclusi /user/eng_test/ERP/dev/deploy/conf/pod_esclusi.txt \
     --DATI /home/eng_test/ERP/dev/output \
     2>&1 | tee ${deploy.path.erp}/logs/erp-dati-$(date +%Y%m%d%H%M%S).log &
   ```
   L'opzione `--DATI` puo' sovrascrivere temporaneamente il path di output indicato in `job.properties`.

3. Output attesi:
   - AC4: genera per ciascun distributore (PIVA diversa da `piva_terna`) un CSV e un XML in `<output>/AC4/<ANNO>/<MESE>/<timestamp>/`, scrivendo i metadati in `ERP_AGGREGATO_PUB`.
   - AC2: analoghi file ma solo per la PIVA TERNA, con metadati su `ERP_AGGREGATO_PUB`.
   - AC4_INT: produce solo gli ZIP di dettaglio POD Interconnessione a partire da `ERP_VALIDATED_INT`, scrivendo i metadati su `ERP_DET_POD_INT_PUB`.

Monitorare i log nella cartella `${deploy.path.erp}/logs` per verificare l'avanzamento e la corretta chiusura dei job.
