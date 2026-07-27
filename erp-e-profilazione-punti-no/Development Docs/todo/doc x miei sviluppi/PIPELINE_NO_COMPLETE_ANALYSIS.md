# 🔴 PIPELINE NO (NON-ORARI) - ANALISI COMPLETA

**Data:** 21 Ottobre 2025  
**Commit:** 51af4ad  
**Status:** ⚠️ BLOCCHI CRITICI IDENTIFICATI - Non pronto per MR

---

## 📊 ARCHITETTURA PIPELINE (4 FASI)

```
INPUT SOURCES
    ↓
    ├─ flussoMisureNoAggr (PNO, RNO, PNO2G, RNO2G, SMIS) 
    ├─ flussiTecnici (AV, AV2G, DS, DS2G - BLACKLISTATI!)
    ├─ flussoMisureSmis (SMIS con smontaggio/montaggio)
    └─ trattPod (per trattamento POD)
    │
    ↓
FASE 1: INGESTION 
    ├─ CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.scala
    │  ├─ 8 metodi trasformazione per flussi diversi
    │  ├─ Estrae totalizzatori EaM, EaF1, EaF2, EaF3
    │  └─ Output → erp_validated_mis_no (tabella intermedia)
    │
    ├─ CalcoloPrelevatoPuntiPrelievoNonOrari.scala
    │  └─ Orchestratore: prepara filtri per anno/mese/area
    │
    ↓
FASE 2: CALCOLO CONSUMO (DELTA TRA TOTALIZZATORI)
    ├─ CalcoloSegmentIConsumoNOTrasformation.scala
    │  ├─ Input: erp_validated_mis_no
    │  ├─ Calcola: Delta = totalizzatore_DX - totalizzatore_SX
    │  │   Per fascie: c_eam, c_eaf1, c_eaf2, c_eaf3
    │  ├─ Normalizza con coefficienti perdita e K
    │  ├─ Valida: scarta delta negativi, flussi errati
    │  └─ Output → erp_consumption_no (tabella intermedia)
    │
    ↓
FASE 3: PROFILAZIONE QUARTORARIA (RIPARTIZIONE NEI 100 QUARTI)
    ├─ ProfilazioneCurveNOTransformation.scala
    │  ├─ Input: erp_consumption_no, daily_dima (fasce orarie)
    │  ├─ Formula: Consumo_QH = Delta_fascia / NqhFascia
    │  │   - valueF0 = c_eam / numq_f0 (monorario)
    │  │   - valueF1 = c_eaf1 / numq_f1 (fascia 1)
    │  │   - valueF2 = c_eaf2 / numq_f2 (fascia 2)
    │  │   - valueF3 = c_eaf3 / numq_f3 (fascia 3)
    │  ├─ Genera 100 quarti per ogni giorno (q1...q100)
    │  ├─ Esplode giorni per intervallo start_giorno...end_giorno
    │  └─ Output → erp_daily_no (100 colonne quarti)
    │
    ↓
FASE 4: AGGREGAZIONE FINALE
    ├─ CalcoloPrelevatoPuntiPrelievoNOTransformation.scala
    │  ├─ Input: erp_daily_no
    │  ├─ Group by: anno, mese, giorno, area, piva_distr, rag_soc_distr
    │  ├─ Sum all quarti q1...q100
    │  └─ Output → erp_aggregato_no (TAVOLA FINALE)
    │
    ↓
OUTPUT
    └─ erp_aggregato_no (100 quarti aggregati per POD, giorno, area)
```

---

## ✅ COSA FUNZIONA CORRETTAMENTE

### FASE 2 - Calcolo Delta (CalcoloSegmentIConsumoNOTrasformation.scala)

**✅ CORRETTO:**
- Linea 112-124: Calcola delta tra totalizzatori DX e SX
  ```scala
  col(c_eam_dx) - col(c_eam_sx)  // Consumo monorario
  col(c_eaf1_dx) - col(c_eaf1_sx)  // Fascia 1
  col(c_eaf2_dx) - col(c_eaf2_sx)  // Fascia 2
  col(c_eaf3_dx) - col(c_eaf3_sx)  // Fascia 3
  ```
- Linea 75-82: Normalizzazione con coeff_perdita e K
- Linea 134-135: Gestione valori negativi (status="ERR_CONS")
- **Conformità D03:** ✅ Corrisponde a sezione 3.2

### FASE 3 - Ripartizione Quartoraria (ProfilazioneCurveNOTransformation.scala)

**✅ CORRETTO:**
- Linea 84-93: Formula esatta come D03 sezione 3.3
  ```scala
  valueF0 = when(useM && numq_f0 > 0, c_eam / numq_f0)     // Consumo_QHMono
  valueF1 = when(useF && numq_f1 > 0, c_eaf1 / numq_f1)   // Consumo_QHF1
  valueF2 = when(useF && numq_f2 > 0, c_eaf2 / numq_f2)   // Consumo_QHF2
  valueF3 = when(useF && numq_f3 > 0, c_eaf3 / numq_f3)   // Consumo_QHF3
  ```
- Linea 102-112: Genera 100 quarti per ogni giorno
  ```scala
  val quarterColumns = (1 to 100).map(i => s"q$i")
  quarterColumns.foldLeft(withArea) { (df, qh) =>
    df.withColumn(qh, quarterValue(qh))
  }
  ```
- **Conformità D03:** ✅ Corrisponde a sezione 3.3 "Ripartizione quartoraria"

### FASE 4 - Aggregazione (CalcoloPrelevatoPuntiPrelievoNOTransformation.scala)

**✅ CORRETTO:**
- Linea 57-67: Group by dimensioni corrette e sum quarti
- Dinamico: lettura di colonne q1...q100 qualunque sia il numero
- **Conformità REQ 2:** ✅ "Archiviare il giorno e i relativi quarti d'ora"

---

## 🔴 PROBLEMI CRITICI - CORRISPONDENZA D03 ROTTA

### ❌ BLOCCO 1: FLUSSI AV, DS, VNO, SNM - COMPLETAMENTE ASSENTI

**D03 Tabella 1 specifica:**
```
Flussi ammissibili per calcolo NO:
✅ PNO, RNO, PNO2G, RNO2G, SMIS (nel codice)
❌ AV, AV2G - MANCANTI
❌ DS, DS2G - MANCANTI  
❌ (VNO, VNO2G, SNM, SNM2G - opzionali ma documentati)
```

**Nel codice - INGESTION (CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.scala):**
- ❌ **NON ESISTE** nessun metodo per elaborare questi flussi
- ❌ **NON SONO** importati da schemi (flussiTeniciSchema contiene solo SMIS)

**Nel codice - CONSUMO (CalcoloSegmentIConsumoNOTrasformation.scala linea 22-23):**
```scala
private val windowErrorSegmentoSinistro  = Seq("SMIS_SMN","DS","DS2G")  // BLACKLISTATI!
private val windowErrorSegmentoDestro    = Seq("SMIS_MN","AV","AV2G")   // BLACKLISTATI!
```

⚠️ **Questi flussi sono MARCATI COME ERRORI (ERR_MIS) anzichè elaborati!**

**Impatto:** Se un POD NO ha dati AV o DS, il flusso di quell'anno/mese viene **SCARTATO** (stato=ERR_MIS).

**Violazione D03:** Articolo 3.1, Tabella 1 - AV e DS sono **AMMISSIBILI**.

---

### ❌ BLOCCO 2: FILTRO TIPODATO TROPPO RESTRITTIVO

**D03 specifica (sezione 3.2, riga 8-10):**
```
Nel caso di flussi periodici può essere sia effettivo che stimato (TipoDato=S/E)
```

**Nel codice - INGESTION (3 occorrenze in CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.scala):**

Linea 56, 105, 137:
```scala
.filter(col(flussoMisureNoAggrSchema.tipodato_e) === Constants.flussiPeriodiciMisuraEffettivaStimataTipoDatoE1)
```

❌ Questo filtro accetta **SOLO** dati effettivi (E1 = "E" = Effettivo)  
❌ **SCARTA COMPLETAMENTE** i dati stimati (S0 = "S" = Stimato)

**Contradd D03:** D03 specifica chiaramente "sia effettivo che stimato"

**Impatto:** Tutti i flussi stimati (durante periodi di guasto/ritardo del misuratore) vengono **IGNORATI** completamente.

**Esempio problematico:**
- Mese febbraio: Distributore trasmette dato stimato (S) per POD X
- Il codice lo scarta
- POD X non viene elaborato per febbraio
- Output finale manca febbraio per quel POD

---

### ❌ BLOCCO 3: FORFAIT E RETTIFICA 4/5 - COMPLETAMENTE MANCANTE

**D03 specifica (sezione 3.2.2, paragrafo intero):**
```
Se GruppoMis="NO" o (GruppoMis="SI" e Forfait="SI"):
Il Distributore comunica direttamente un valore di "Consumo" 
(SEZIONE Consumo) invece di "Misura" (totalizzatore)

Procedura:
1. Leggere il campo "Consumo" dal flusso
2. A partire dalla "DataInizioPeriodo"
3. Ripartire il Consumo nei quarti d'ora del mese
```

**Nel codice:**
- ❌ **ZERO handling** per il caso Forfait/Rettifica 4/5
- ❌ **NON ESISTE** nessun check per GruppoMis="NO"
- ❌ **NON ESISTE** parsing del campo "Consumo"
- ❌ **NON ESISTE** logica di ripartizione per consumo diretto

**Impatto:** Tutti i POD con forfait vengono **SCARTATI** o **IGNORATI**.

**Caso d'uso reale (D03 Tabella 3-4):**
```
30/04/2024: RNO con Consumo=150 (forfait), DataInizioPeriodo=01/04
Il consumo 150 DEVE essere ripartito nei 30 quarti d'ora di aprile
Consumo_QH_aprile = 150 / 30 = 5 kWh per quarto

Nel codice: QUESTO NON ACCADE - il record viene scartato!
```

---

### ⚠️ BLOCCO 4: SMIS - GESTIONE INCOMPLETA

**D03 specifica (sezione 3.2.1):**
```
In caso di SMIS (smontaggio/montaggio misuratore):
- Delta PRE = Totalizzatore_smontaggio - totalizzatore_precedente
- Delta POST = Totalizzatore_attuale - totalizzatore_montaggio
- Consumo_finale = Delta PRE + Delta POST
```

**Esempio D03 (Tabella 3-4):**
```
Data     | Flusso | Totalizzatore | Consumo
31/01    | PNO    | T1=1100       |
28/02    | PNO    | T2=1300       | ← Delta PRE = 1300-1100 = 200
31/03    | PNO    | T3=1500       | ← (mese successivo smis)
30/04    | RNO    | (SMIS)        | ← Consumo1=150 (forfait)
31/05    | RNO    | (SMIS)        | ← Consumo2=160 (forfait)
30/06    | PNO    | T4=2000       | ← Delta POST = 2000-(1500+150+160) = 190
```

**Nel codice (CalcoloSegmentIConsumoNOTrasformation.scala):**
- ✅ Calcola `eam_dx - eam_sx` correttamente
- ❌ **NON VEDO** il calcolo del Delta PRE e POST separati per SMIS
- ❌ Tratta SMIS come un segmento normale invece di due segmenti
- ❌ **MANCA** la logica di conguaglio nel mese successivo

**Impatto:** I mesi con SMIS potrebbero avere calcoli di consumo **IMPRECISI**.

---

## 📋 TABELLA CONFORMITÀ D03 vs CODICE

| Requisito D03 | Linea Codice | Status | Dettagli |
|---|---|---|---|
| **Sezione 3.1 - Flussi ammissibili** | CalcoloPrelevatoPuntiPrelievoMisNOTrasformation | ❌ INCOMPLETO | PNO,SMIS ✅; AV,DS ❌ BLACKLISTATI |
| **Sezione 3.2 - Delta tra totalizzatori** | CalcoloSegmentIConsumoNOTrasformation:112-124 | ✅ CORRETTO | Formula esatta: dx - sx |
| **Sezione 3.2 - TipoDato (S/E)** | CalcoloPrelevatoPuntiPrelievoMisNOTrasformation:56 | ❌ ERRATO | Filtro solo E, scarta S |
| **Sezione 3.2.1 - SMIS PRE/POST** | CalcoloSegmentIConsumoNOTrasformation | ⚠️ INCOMPLETO | Non vedo logica PRE/POST |
| **Sezione 3.2.2 - Forfait/Rett 4/5** | N/A | ❌ ASSENTE | Zero handling |
| **Sezione 3.3 - Ripartizione Q H** | ProfilazioneCurveNOTransformation:84-93 | ✅ CORRETTO | Formula: Delta/NqhFascia |
| **Sezione 3.3 - 100 quarti** | ProfilazioneCurveNOTransformation:102-112 | ✅ CORRETTO | Explode + sequence |
| **REQ 2 - Metadati archiviati** | ProfilazioneCurveNOTransformation | ✅ CORRETTO | Pod, area, anno, mese, giorno, quarti |

---

## 🎯 RIASSUNTO CORRISPONDENZA

### ✅ CORRETTO (60%)
- ✅ Calcolo delta tra totalizzatori (D03 sezione 3.2)
- ✅ Ripartizione quartoraria (D03 sezione 3.3)
- ✅ Generazione 100 quarti
- ✅ Aggregazione finale
- ✅ Archiviazione metadati

### ❌ ERRATO/MANCANTE (40%)
- ❌ Flussi AV, DS: BLACKLISTATI anzichè elaborati
- ❌ TipoDato: Filtro restrittivo (solo E, non S)
- ❌ Forfait: COMPLETAMENTE ASSENTE
- ⚠️  SMIS: Logica PRE/POST INCOMPLETA

---

## 📝 RACCOMANDAZIONI PER MR

### Priority 1 (CRITICO - Blocca MR)

**1. Aggiungere elaborazione flussi AV, DS**
```
File: CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.scala
Action: Creare 2 metodi per AV/AV2G e DS/DS2G come PNO
Impact: REQ 2 coperto completamente
```

**2. Rimuovere blacklist AV, DS da CalcoloSegmentIConsumoNOTrasformation.scala**
```
File: CalcoloSegmentIConsumoNOTrasformation.scala, linea 22-23
Action: Rimuovere "DS","DS2G" da windowErrorSegmentoSinistro
Action: Rimuovere "AV","AV2G" da windowErrorSegmentoDestro
Impact: AV/DS non più marcati come errori
```

**3. Estendere filtro TipoDato per accettare sia E che S**
```
File: CalcoloPrelevatoPuntiPrelievoMisNOTrasformation.scala, linea 56, 105, 137
Old: .filter(col(tipodato_e) === E1)
New: .filter(col(tipodato_e).isin(E1, S1) OR tipodato_s.isNotNull)
Impact: Conformità D03 sezione 3.2
```

### Priority 2 (IMPORTANTE - Deve essere in MR)

**4. Aggiungere handling Forfait/Rettifica 4/5**
```
File: CalcoloSegmentIConsumoNOTrasformation.scala
Action: Aggiungere case per GruppoMis="NO"
Action: Leggere campo "Consumo" dal flusso
Action: Calcolare DataInizioPeriodo come start
Impact: D03 sezione 3.2.2 coperta
```

### Priority 3 (MIGLIORAMENTO - Post MR)

**5. Validare logica SMIS PRE/POST**
```
File: CalcoloSegmentIConsumoNOTrasformation.scala
Action: Verificare se logica PRE/POST è implementata
Action: Se no, aggiungere handling specifico per SMIS
Impact: D03 sezione 3.2.1 completamente coperta
```

---

## 🔗 DOCUMENTAZIONE RIFERIMENTI

- **D03 (Autorità):** "Allegato al Processo Trasmissione Misure - profilazione quartoraria_20250413"
- **REQ 2:** erp_calculation_specs.md - Profilazione Quartoraria dei punti Non Orari
- **Delibera:** ARERA 325/2024/R/EEL

---

## ✋ CONCLUSIONE

**La pipeline NO è FUNZIONANTE al 60% ma ha BLOCCHI CRITICI che violano D03.**

**Prima di MR:**
1. ❌ Aggiungere flussi AV, DS (attualmente BLACKLISTATI)
2. ❌ Estendere TipoDato per accettare Stimati
3. ❌ Implementare Forfait/Rettifica 4/5
4. ⚠️  Validare SMIS PRE/POST

**Senza questi fix, la MR sarà RIFIUTATA per non conformità a D03.**
