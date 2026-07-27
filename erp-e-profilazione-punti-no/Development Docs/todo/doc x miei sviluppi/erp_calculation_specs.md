# Procedura di Calcolo Energia Residuale Parziale - ERP

## Requisito 1: Calcolo ERP

Si richiede lo sviluppo di un motore di calcolo che utilizzi la tecnologia Spark2 e sia in grado di calcolare l'Energia Residuale Parziale (ERP) per singolo gestore di rete e zona. Il dato di ERP deve essere messo a disposizione di Terna e di tutti i distributori tramite la generazione di file aggregati secondo nomenclatura e tracciato indicata nella relativa Specifica pubblicata sul portale del SII.

### Informazioni Necessarie per il Calcolo

Per il calcolo di ERP è necessario utilizzare come informazioni:

**1. Energia immessa e prelevata relativa ai punti di interconnessione:**

Il Gestore di rete responsabile della misura trasmette al SII l'energia immessa e prelevata relative ai punti di interconnessione, con curva quartoraria giornaliera, tramite il flusso di tipo "INT". Questo è caricato in apposite tabelle.

**2. Energia Prelevata relativa ai punti di prelievo ordinari orari:**

Per l'energia dei punti di prelievo orari è possibile utilizzare l'informazione aggregata sui distributori e archiviata nelle rispettive tabelle. Per l'aggregato ERP di Terna sarà necessario individuare i POD sulle tabelle di dettaglio, a seguito dell'individuazione dell'insieme dei predetti POD Orari nel RCU afferenti a Terna.

**3. Energia Prelevata relativa ai punti di prelievo ordinari non orari:**

Per ricavare l'energia prelevata relativa ai punti di prelievo non orario è necessario individuare il perimetro dei pod e flussi da utilizzare, calcolare il consumo e ripartire quest'ultimo in curve quartorarie, come indicato nel requisito 2.

**4. Energia Prelevata relativa ai punti di prelievo illuminazione pubblica:**

Per l'energia dei punti di illuminazione IP è possibile utilizzare l'informazione aggregata sui distributori e archiviata nelle tabelle funzionali all'aggregazione IP.

### Parametri di Input

Il motore di calcolo dovrà calcolare l'ERP a partire da un **annomese in input (obbligatorio)**.

Si richiede inoltre come input facoltativo per il calcolo:
- La PIVA del distributore
- L'area

---

## Requisito 2: Profilazione Quartoraria dei punti Non Orari

Si richiede una funzionalità di profilazione Quartoraria dei punti Non Orari sulla base del seguente perimetro:

1. POD che all'interno di RCU presentano trattamento diverso da O
2. La relativa Area geografica
3. I flussi ammissibili di tipo: PNO, PNO2G, SMIS, AV, AV2G, DS, DS2G, VNO, VNO2G, SNM, SNM2G (RNO, RNO2g disabilitatili)

### Informazioni da Archiviare

Si richiede di archiviare non solo l'informazione della profilazione, ma anche:

1. Il trattamento del POD
2. L'area
3. L'annomese di profilazione
4. Il giorno di profilazione e i relativi quarti d'ora
5. Consumo e letture utilizzate per il calcolo del consumo
6. Le date delle predette letture
7. I nomi dei flussi utilizzati per il calcolo dei consumi
8. Il tipo di lettura del flusso

### Parametri di Input

Si richiede la possibilità di invocare la funzionalità non solo su un determinato annomese, ma anche su alcuni input facoltativi:
- La PIVA del distributore
- L'area

Si richiede una funzionalità di esclusione di alcuni pod dal perimetro di cui sopra.

---

## Requisito 3: Generazione dettagli POD di Interconnessione

Si richiede una funzionalità di generazione dei dettagli POD, relativamente ai POD di interconnessione secondo nomenclatura e tracciato indicato nella relativa Specifica pubblicata sul portale del SII.

Le informazioni sottese alla generazione dei dettagli dovranno essere archiviate in apposite tabelle, al fine di consultazione/monitoraggio.

---

## Requisito 4: Messa a disposizione dei dati di ERP e dettagli ERP

Si richiede la possibilità di mettere a disposizione i dati dell'ERP e dettagli POD interconnessione in una specifica staging area, successivamente allineata nell'alberatura AC del SIICLOUDPROCESSI.

---

## Requisiti Prestazionali

Si richiede una tempistica di elaborazione:

- **Per il REQ 1:** pari a 60 minuti per l'elaborazione di circa 20.000 POD di interconnessione
- **Per il REQ 2:** pari a 60 minuti per l'elaborazione di circa 5 milioni di POD non orari

È prevista una tolleranza in eccesso sulle predette tempistiche del 50%.