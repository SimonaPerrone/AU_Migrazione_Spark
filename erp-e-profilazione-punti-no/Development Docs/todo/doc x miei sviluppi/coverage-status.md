# Coverage Status – ERP Project (Oct 2025)

## Executive Summary
- La suite di test esistente copre ancora solo i flussi storici (DIST, TERNA, IP, INT) e l’ingestion del flusso NO.
- Le nuove componenti NO (segmenti, profilazione, aggregazione) non hanno test automatici.
- Mancano totalmente test per l’orchestrazione Main e per i processi di saldo/export, tuttora non implementati.

## Test presenti
| Ambito | File | Note |
| ------ | ---- | ---- |
| Supporto Spark locale | src/test/scala/it/eng/au/ERP/EnvironmentSparkTest.scala | Inizializza Spark local[*] con params.properties. |
| DIST | low/DIST/DISTCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest.scala | Verifica aggregazione quartoraria DIST. |
| TERNA | low/TERNA/TERNACalcoloPrelevatoPuntiPrelievoOrariTrasformationTest.scala | Variante TERNA (T_CONNESSIONE ≠ N). |
| IP | low/IP/IPCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest.scala | Copre trasformazione su V_AGGREGAZIONE_MISURE_IP. |
| INT (ingestion) | low/INT/INTCalcoloPrelevatoPuntiPrelievoOrariTest.scala | Testa pipeline ingestion/interconnessione. |
| INT (trasformazioni) | low/INT/INTCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest.scala | Dettaglio e aggregato INT. |
| NO – ingestion | low/NO/Ingestion/flussiPeriodiciTest.scala | Esercita le funzioni helper per flussi periodici/tecnici. |

## Gap di test
- **NO – Segmenti**: nessun test per CalcoloSegmentiConsumoNOFlow / CalcoloSegmentIConsumoNOTrasformation (stati ERR_*, calcolo c_eam/c_eaf*).
- **NO – Profilazione**: nessuna suite per CalcoloProfilazioneCurveNOFlow / ProfilazioneCurveNOTransformation (distribuzione quartoraria con DIMA, join RCU). **Priorità alta**.
- **NO – Aggregazione**: zero test per CalcoloPrelevatoPuntiPrelievoNonOrari (aggregato NO).
- **Flow orchestrators**: nessun test integrato su CalcoloEnergiaImmessaPrelevataNOFlow o sul dispatcher Main (Casi DIST/TERNA/IP/INT/FULL).
- **Componenti mancanti**: saldo ERP ed export sono ancora da sviluppare, quindi mancano sia il codice sia i relativi test.

## Prossimi passi suggeriti
1. Scrivere test unitari per segmentazione NO (casi OK / errori) con dataset sintetici.
2. Aggiungere suite per la profilazione NO, generando un mock di ERP_DAILY_DIMA.
3. Coprire l’aggregazione NO con test Spark (somma QH per area/PIVA).
4. Pianificare test end-to-end per Main (almeno mocking dei DAO) una volta stabilizzati i flussi.
5. Quando il saldo/export sarà implementato, definire casi di prova e fixture dedicate.
